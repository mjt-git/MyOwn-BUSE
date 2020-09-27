/*
 * RAID1 example for BUSE
 * by Tyler Bletsch to ECE566, Duke University, Fall 2019
 * 
 * Based on 'busexmp' by Adam Cozzette
 *
 * This program is free software; you can redistribute it and/or modify
 *    it under the terms of the GNU General Public License as published by
 *    the Free Software Foundation; either version 2 of the License, or
 *    (at your option) any later version.
 */
 
#define _GNU_SOURCE
#define _LARGEFILE64_SOURCE

#include <argp.h>
#include <err.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <stdbool.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <assert.h>
#include <unistd.h>

#include "buse.h"

#define UNUSED(x) (void)(x) // used to suppress "unused variable" warnings without turning off the feature entirely

int num_devices = 0;
int dev_fd[16]; // file descriptors for two underlying block devices that make up the RAID
int block_size;  //NOTE: other than truncating the resulting raid device, block_size is ignored in this program; it is asked for and set in order to make it easier to adapt this code to RAID0/4/5/6.
uint64_t raid_device_size; // size of raid device in bytes
bool verbose = false;  // set to true by -v option for debug output
bool degraded = false; // true if we're missing a device

int ok_dev = -1; // index of dev_fd that has a valid drive (used in degraded mode to identify the non-missing drive (0 or 1))
int rebuild_dev = -1; // index of drive that is being added with '+' for RAID rebuilt

int last_read_dev = 0; // used to interleave reading between the two devices

// XOR buf1 and buf2, store result in buf1
void bigxor(int8_t * buf1, int8_t * buf2) {
    for(int i = 0; i < block_size; ++i) {
        buf1[i] = buf1[i] ^ buf2[i];
    }
}

// calculate missed block data from XORing all other blocks
void * getMissedBlk(u_int32_t on_device_blk_idx) {
    void * buf1 = malloc(block_size);
    void * buf2 = malloc(block_size);
    int buf1_inited = 0;

    for(int i = 0; i < num_devices; ++i) {
        if(dev_fd[i] != -1) {
            if(buf1_inited == 0) {
                pread(dev_fd[i], buf1, block_size, on_device_blk_idx * block_size);
                buf1_inited = 1;
            } else {
                pread(dev_fd[i], buf2, block_size, on_device_blk_idx * block_size);
                bigxor(buf1, buf2);
            }
        }
    }

    free(buf2);
    return buf1;
}

static int xmp_read(void *buf, u_int32_t len, u_int64_t offset, void *userdata) {
    UNUSED(userdata);
    if (verbose)
        fprintf(stderr, "R - %lu, %u\n", offset, len);

    u_int32_t blk_num = offset / block_size;
    u_int32_t device_idx = blk_num % (num_devices - 1);
    u_int32_t on_device_blk_idx = blk_num / (num_devices - 1);
    u_int64_t offset_on_blk = offset % block_size;
    u_int64_t device_offset = on_device_blk_idx * block_size + offset_on_blk;
    u_int32_t len_tobe_read = len <= block_size - offset_on_blk ? len : block_size - offset_on_blk;

    if(dev_fd[device_idx] != -1) {
        pread(dev_fd[device_idx], buf, len_tobe_read, device_offset);
        fprintf(stderr, "pread calles, drive_num: %d, len: %u, offset: %lu\n", device_idx, len_tobe_read, device_offset);
    } else {
        void * blk_calced = getMissedBlk(on_device_blk_idx);
        memcpy(buf, blk_calced, len_tobe_read);
        free(blk_calced);
    }
    len -= len_tobe_read;
    buf += len_tobe_read;
    while(len > 0) {
        blk_num++;
        device_idx = blk_num % (num_devices - 1);
        on_device_blk_idx = blk_num / (num_devices - 1);
        device_offset = on_device_blk_idx * block_size;
        len_tobe_read = len <= (u_int32_t)block_size ? len : (u_int32_t)block_size;

        if(dev_fd[device_idx] != -1) {
            pread(dev_fd[device_idx], buf, len_tobe_read, device_offset);
            fprintf(stderr, "pread calles, drive_num: %d, len: %u, offset: %lu\n", device_idx, len_tobe_read, device_offset);
        } else {
            void * blk_calced = getMissedBlk(on_device_blk_idx);
            memcpy(buf, blk_calced, len_tobe_read);
            free(blk_calced);
        }
        len -= len_tobe_read;
        buf += len_tobe_read;
    }

    return 0;
}

void get_new_parity_blk(int8_t * new_blk, int8_t * old_blk, int8_t * parity_blk) {
    for(int i = 0; i < block_size; ++i) {
        parity_blk[i] = parity_blk[i] ^ old_blk[i];
        parity_blk[i] = parity_blk[i] ^ new_blk[i];
    }
}

void write_into_blk(const void *buf, int device_idx, u_int32_t len_tobe_write, u_int64_t device_offset, u_int32_t on_device_blk_idx) {
    void * old_blk = malloc(block_size);
    void * parity_blk = malloc(block_size);
    pread(dev_fd[device_idx], old_blk, block_size, on_device_blk_idx * block_size);
    pread(dev_fd[num_devices-1], parity_blk, block_size, on_device_blk_idx * block_size);

    pwrite(dev_fd[device_idx], buf, len_tobe_write, device_offset);
    fprintf(stderr, "pwrite calles, drive_num: %d, len: %u, offset: %lu\n", device_idx, len_tobe_write, device_offset);
    void * new_blk = malloc(block_size);
    pread(dev_fd[device_idx], new_blk, block_size, on_device_blk_idx * block_size);
    get_new_parity_blk(new_blk, old_blk, parity_blk);

    pwrite(dev_fd[num_devices-1], parity_blk, block_size, on_device_blk_idx * block_size);
    fprintf(stderr, "pwrite calles, drive_num: %d, len: %u, offset: %d\n", num_devices-1, block_size, 0);

    free(old_blk);
    free(parity_blk);
    free(new_blk);
}

// the drive we are writing is missing, so only need to update parity block
void write_on_missed(const void *buf, u_int32_t on_device_blk_idx, u_int64_t offset_on_blk, u_int32_t len_tobe_write) {
    void * old_blk = getMissedBlk(on_device_blk_idx);
    void * new_blk = malloc(block_size);
    memcpy(new_blk, old_blk, block_size);
    memcpy(new_blk + offset_on_blk, buf, len_tobe_write);
    void * parity_blk = malloc(block_size);
    pread(dev_fd[num_devices-1], parity_blk, block_size, on_device_blk_idx * block_size);

    get_new_parity_blk(new_blk, old_blk, parity_blk);
    pwrite(dev_fd[num_devices-1], parity_blk, block_size, on_device_blk_idx * block_size);
    fprintf(stderr, "pwrite calles, drive_num: %d, len: %u, offset: %u\n", num_devices-1, block_size, on_device_blk_idx * block_size);

    free(old_blk);
    free(new_blk);
    free(parity_blk);
}

static int xmp_write(const void *buf, u_int32_t len, u_int64_t offset, void *userdata) {
    UNUSED(userdata);
    if (verbose)
        fprintf(stderr, "W - %lu, %u\n", offset, len);
    
    u_int32_t blk_num = offset / block_size;
    u_int32_t device_idx = blk_num % (num_devices - 1);
    u_int32_t on_device_blk_idx = blk_num / (num_devices - 1);
    u_int64_t offset_on_blk = offset % block_size;
    u_int64_t device_offset = on_device_blk_idx * block_size + offset_on_blk;
    u_int32_t len_tobe_write = len <= block_size - offset_on_blk ? len : block_size - offset_on_blk;

    if(degraded) {
        // 1) the drive we are writing is missing, so only need to update parity block
        // 2) the drive we are writing is not missing, some other data drive missed.  so this is same as "else"
        // 3) the drive we are writing is not missing, parity drive missed, so directly write on it.
        if(dev_fd[device_idx] == -1) {
            write_on_missed(buf, on_device_blk_idx, offset_on_blk, len_tobe_write);
        } else if(dev_fd[num_devices-1] == -1) {
            pwrite(dev_fd[device_idx], buf, len_tobe_write, device_offset);
            fprintf(stderr, "pread calles, drive_num: %d, len: %u, offset: %lu\n", device_idx, len_tobe_write, device_offset);
        } else {
            write_into_blk(buf, device_idx, len_tobe_write, device_offset, on_device_blk_idx);
        }
        len -= len_tobe_write;
        buf += len_tobe_write;
        while(len > 0) {
            blk_num++;
            device_idx = blk_num % (num_devices - 1);
            on_device_blk_idx = blk_num / (num_devices - 1);
            device_offset = on_device_blk_idx * block_size;
            len_tobe_write = len <= (u_int32_t)block_size ? len : (u_int32_t)block_size;

            if(dev_fd[device_idx] == -1) {
                write_on_missed(buf, on_device_blk_idx, 0, len_tobe_write);
            } else if(dev_fd[num_devices-1] == -1) {
                pwrite(dev_fd[device_idx], buf, len_tobe_write, device_offset);
                fprintf(stderr, "pread calles, drive_num: %d, len: %u, offset: %lu\n", device_idx, len_tobe_write, device_offset);
            } else {
                write_into_blk(buf, device_idx, len_tobe_write, device_offset, on_device_blk_idx);
            }
            len -= len_tobe_write;
            buf += len_tobe_write;
        }

    } else {
        write_into_blk(buf, device_idx, len_tobe_write, device_offset, on_device_blk_idx);
        len -= len_tobe_write;
        buf += len_tobe_write;
        while(len > 0) {
            blk_num++;
            device_idx = blk_num % (num_devices - 1);
            on_device_blk_idx = blk_num / (num_devices - 1);
            device_offset = on_device_blk_idx * block_size;
            len_tobe_write = len <= (u_int32_t)block_size ? len : (u_int32_t)block_size;

            write_into_blk(buf, device_idx, len_tobe_write, device_offset, on_device_blk_idx);
            
            len -= len_tobe_write;
            buf += len_tobe_write;
        }
    }

    return 0;
}

static int xmp_flush(void *userdata) {
    UNUSED(userdata);
    if (verbose)
        fprintf(stderr, "Received a flush request.\n");
    for (int i=0; i<num_devices; i++) {
        if (dev_fd[i] != -1) { // handle degraded mode
            fsync(dev_fd[i]); // we use fsync to flush OS buffers to underlying devices
        }
    }
    return 0;
}

static void xmp_disc(void *userdata) {
    UNUSED(userdata);
    if (verbose)
        fprintf(stderr, "Received a disconnect request.\n");
    // disconnect is a no-op for us
}

/*
// we'll disable trim support, you can add it back if you want it
static int xmp_trim(u_int64_t from, u_int32_t len, void *userdata) {
    UNUSED(userdata);
    if (verbose)
        fprintf(stderr, "T - %lu, %u\n", from, len);
    // trim is a no-op for us
    return 0;
}
*/

/* argument parsing using argp */

static struct argp_option options[] = {
    {"verbose", 'v', 0, 0, "Produce verbose output", 0},
    {0},
};

struct arguments {
    uint32_t block_size;
    char* device[16];
    char* raid_device;
    int verbose;
};

/* Parse a single option. */
static error_t parse_opt(int key, char *arg, struct argp_state *state) {
    struct arguments *arguments = state->input;
    char * endptr;

    switch (key) {

        case 'v':
            arguments->verbose = 1;
            break;

        case ARGP_KEY_ARG:
            switch (state->arg_num) {

                case 0:
                    arguments->block_size = strtoul(arg, &endptr, 10);
                    if (*endptr != '\0') {
                        /* failed to parse integer */
                        errx(EXIT_FAILURE, "SIZE must be an integer");
                    }
                    break;

                case 1:
                    arguments->raid_device = arg;
                    break;
                    
                default:
                    if(state->arg_num <= 17 && state->arg_num >= 2) {
                        num_devices = state->arg_num - 1 > (uint32_t)num_devices ? state->arg_num - 1 : (uint32_t)num_devices;
                        arguments->device[state->arg_num-2] = arg;
                    } else {
                        return ARGP_ERR_UNKNOWN;    
                    }
                    
            }
            break;

        case ARGP_KEY_END:
            if (state->arg_num < 4 || state->arg_num > 17) {
                warnx("Wrong argument number! Drive numbers should between 3 and 16");
                argp_usage(state);
            }
            break;

        default:
            return ARGP_ERR_UNKNOWN;
    }
    return 0;
}

static struct argp argp = {
    .options = options,
    .parser = parse_opt,
    .args_doc = "BLOCKSIZE RAIDDEVICE DEVICE1 DEVICE2 DEVICE3 ...",
    .doc = "BUSE implementation of RAID4 for 3 ~ 16 devices.\n"
           "`BLOCKSIZE` is an integer number of bytes. "
           "\n\n"
           "`RAIDDEVICE` is a path to an NBD block device, for example \"/dev/nbd0\"."
           "\n\n"
           "`DEVICE*` is a path to underlying block devices. Normal files can be used too. A `DEVICE` may be specified as \"MISSING\" to run in degraded mode. "
           "\n\n"
           "If you prepend '+' to a DEVICE, you are re-adding it as a replacement to the RAID, and we will rebuild the array. "
           "This is synchronous; the rebuild will have to finish before the RAID is started. "
};

static int do_raid_rebuild() {
    uint32_t blk_count = raid_device_size / block_size;
    for(u_int32_t i = 0; i < blk_count; ++i) {
        void * blk_data = getMissedBlk(i);
        pwrite(dev_fd[rebuild_dev], blk_data, block_size, i * block_size);
        free(blk_data);
    }

    return 0;
}

int main(int argc, char *argv[]) {
    struct arguments arguments = {
        .verbose = 0,
    };
    argp_parse(&argp, argc, argv, 0, 0, &arguments);
    
    struct buse_operations bop = {
        .read = xmp_read,
        .write = xmp_write,
        .disc = xmp_disc,
        .flush = xmp_flush,
        // .trim = xmp_trim, // we'll disable trim support, you can add it back if you want it
    };

    verbose = arguments.verbose;
    block_size = arguments.block_size;
    
    raid_device_size=0; // will be detected from the drives available
    ok_dev=-1;
    bool rebuild_needed = false; // will be set to true if a drive is MISSING
    for (int i=0; i<num_devices; i++) {
        char* dev_path = arguments.device[i];
        if (strcmp(dev_path,"MISSING")==0) {
            degraded = true;
            dev_fd[i] = -1;
            fprintf(stderr, "DEGRADED: Device number %d is missing!\n", i);
        } else {
            if (dev_path[0] == '+') { // RAID rebuild mode!!
                if (rebuild_needed) {
                    // multiple +drives detected
                    fprintf(stderr, "ERROR: Multiple '+' drives specified. Can only recover one drive at a time.\n");
                    exit(1);
                }
                dev_path++; // shave off the '+' for the subsequent logic
                rebuild_dev = i;
                rebuild_needed = true;
            }
            ok_dev = i;
            dev_fd[i] = open(dev_path,O_RDWR);
            if (dev_fd[i] < 0) {
                perror(dev_path);
                exit(1);
            }
            uint64_t size = lseek(dev_fd[i],0,SEEK_END); // used to find device size by seeking to end
            fprintf(stderr, "Got device '%s', size %ld bytes.\n", dev_path, size);
            if (raid_device_size==0 || size<raid_device_size) {
                raid_device_size = size; // raid_device_size is minimum size of available devices
            }
        }
    }
    
    raid_device_size = raid_device_size/block_size*block_size; // divide+mult to truncate to block size
    bop.size = raid_device_size; // tell BUSE how big our block device is
    if (rebuild_needed) {
        if (degraded) {
            fprintf(stderr, "ERROR: Can't rebuild from a missing device (i.e., you can't combine MISSING and '+').\n");
            exit(1);
        }
        fprintf(stderr, "Doing RAID rebuild...\n");
        if (do_raid_rebuild() != 0) { 
            // error on rebuild
            fprintf(stderr, "Rebuild failed, aborting.\n");
            exit(1);
        }
    }
    if (degraded && ok_dev==-1) {
        fprintf(stderr, "ERROR: No functioning devices found. Aborting.\n");
        exit(1);
    }
    fprintf(stderr, "RAID device resulting size: %ld.\n", bop.size);
    
    return buse_main(arguments.raid_device, &bop, NULL);
}
