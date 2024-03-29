
#ifndef LOCALTHESIS_MESSAGESENUM_H
#define LOCALTHESIS_MESSAGESENUM_H

enum MessagesEnum{


    // node leaving the cluster

    NODE_LEAVING_THE_CLUSTER_LEAVE,
    CONSISTENT_HASHING_INFORMATION_SYNCED_LEAVE,
    UNION_FIND_DATA_LEAVE,
    UNION_FIND_DATA_SEND_MORE,
    UNION_FIND_NODE_RECEIVED_ALL_LEAVE,
    BUCKET_AMOUNTS_DATA_LEAVE,
    BUCKETS_AMOUNTS_APPROVED_LEAVE,

    // buckets sending
    INCOMING_SHUFFLED_BUCKET_DATA,
    INCOMING_SHUFFLED_BUCKET_DATA_SEND_MORE,
    INCOMING_SHUFFLED_BUCKET_DATA_RECEIVED_ALL,
    ADD_PAGE_ID_TO_BUCKET,
    BUCKET_MOVED_TO_NEW_NODE,
    NODE_FINISHED_RECEIVING_BUCKETS,

};

#endif //LOCALTHESIS_MESSAGESENUM_H
