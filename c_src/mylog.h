#pragma once

#define _GNU_SOURCE

#define FG_BLACK        30
#define FG_RED          31
#define FG_GREEN        32
#define FG_YELLOW       33
#define FG_BLUE         34
#define FG_MAGENTA      35
#define FG_CYAN         36
#define FG_WHITE        37
#define BG_BLACK        40
#define BG_RED          41
#define BG_GREEN        42
#define BG_YELLOW       43
#define BG_BLUE         44
#define BG_MAGENTA      45
#define BG_CYAN         46
#define BG_WHITE        47
#define B_RED(str)      "\033[1;31m" str "\033[0m"
#define B_GREEN(str)    "\033[1;32m" str "\033[0m"
#define B_YELLOW(str)   "\033[1;33m" str "\033[0m"
#define B_BLUE(str)     "\033[1;34m" str "\033[0m"
#define B_MAGENTA(str)  "\033[1;35m" str "\033[0m"
#define B_CYAN(str)     "\033[1;36m" str "\033[0m"
#define B_WHITE(str)    "\033[1;37m" str "\033[0m"
#define RED(str)        "\033[31m" str "\033[0m"
#define GREEN(str)      "\033[32m" str "\033[0m"
#define YELLOW(str)     "\033[33m" str "\033[0m"
#define BLUE(str)       "\033[34m" str "\033[0m"
#define MAGENTA(str)    "\033[35m" str "\033[0m"
#define CYAN(str)       "\033[36m" str "\033[0m"
#define WHITE(str)      "\033[37m" str "\033[0m"
#define GREY(str)       "\033[30;1m" str "\033[0m"

#ifdef __APPLE__
#include <pthread.h>
#define thrd_id() ({\
        uint64_t tid;\
        pthread_threadid_np(NULL, &tid);\
        tid;\
        })

#else
#include <unistd.h>

#include <sys/syscall.h>
#include <sys/types.h>

#define thrd_id() ({ \
            syscall(SYS_gettid);\
        })
#endif


#ifdef MYDEBUG

#define DBG(fmt, ...)
#define WARN(fmt, ...)

#else

#define DBG(fmt, ...) enif_fprintf(stdout, GREY("[%34s#%-5d@(tid:%llx)]") " " fmt "\r\n", __FUNCTION__,__LINE__,thrd_id(), ##__VA_ARGS__)
#define WARN(fmt, ...) enif_fprintf(stdout, B_MAGENTA("[%34s#%-5d@(tid:%llx)]") " " fmt "\r\n", __FUNCTION__,__LINE__,thrd_id(), ##__VA_ARGS__)

#endif

#define INFO_LOG(fmt, ...) enif_fprintf(stderr, "[@(tid:%llx)] " fmt "\r\n", thrd_id(), ##__VA_ARGS__)
#define WARN_LOG(fmt, ...) enif_fprintf(stderr, B_YELLOW("[@(tid:%llx)] " fmt) "\r\n", thrd_id(), ##__VA_ARGS__)
#define ERR_LOG(fmt, ...) enif_fprintf(stderr, B_RED("[@(tid:%llx)] " fmt) "\r\n", thrd_id(), ##__VA_ARGS__)
