//
// Created by YuvalFreund on 20.01.24.
//

#ifndef LOCALTHESIS_BUCKETMANAGERDEFS_H
#define LOCALTHESIS_BUCKETMANAGERDEFS_H

//Macros
#define MAX_BUCKETS 420
#define BUCKETS_NUM_TO_INIT 62
#define SLOT_SIZE_IN_BYTE 65536
#define BUCKET_ALREADY_MERGED 1000000000
#define INVALID_NODE_ID 1000000000
#define MAX_TRY_TO_ADD_TO_BUCKET 5
#define ZERO 0
#define CONSISTENT_HASHING_WEIGHT 10
#define MAX_PAGES_5_BYTES_BUCKET_ID 16777216
#define MAX_PAGES_6_BYTES_BUCKET_ID 65536
#define MAX_PAGES_7_BYTES_BUCKET_ID 256
#define BUCKET_ID_MASK_5_BYTES_BUCKET_ID 0xFFFFFFFFFF000000
#define BUCKET_ID_MASK_6_BYTES_BUCKET_ID 0xFFFFFFFFFFFF0000
#define BUCKET_ID_MASK_7_BYTES_BUCKET_ID 0xFFFFFFFFFFFFFF00
#define MESSAGE_SIZE 64
#define MSG_ENUM_IDX 0
#define MSG_SND_IDX 1
#define MSG_RCV_IDX 2
#define MSG_DATA_START_IDX 4
#define CONSENSUS_VEC_SIZE 3
#define BUCKET_ID_START_INDEX 16
#define NODE_ID_START_INDEX 24
#define PAGE_ID_START_INDEX 24
#define SSD_SLOT_START_INDEX 24
#define UNION_FIND_BUCKET_DATA_SENT_IDX 3
#define UNION_FIND_DATA_MAX_AMOUNT 5
#define MESSAGE_ENUM_AMOUNT 25





#endif //LOCALTHESIS_BUCKETMANAGERDEFS_H
