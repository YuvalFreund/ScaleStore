//
// Created by YuvalFreund on 28.11.23.
//

#ifndef LOCALTHESIS_BUCKETMESSAGE_H
#define LOCALTHESIS_BUCKETMESSAGE_H

#include <cstddef>
#include <cstdint>
#include "MessagesEnum.h"

#include "BucketManagerDefs.h"

struct BucketMessage {
    uint8_t messageData[MESSAGE_SIZE];
    MessagesEnum messageEnum;

    explicit BucketMessage(uint8_t *newMessageData) {
        for (int i = 0; i < MESSAGE_SIZE; i++) {
            this->messageData[i] = newMessageData[i];
        }
        messageEnum = static_cast<MessagesEnum>((int) messageData[MSG_ENUM_IDX]);
    }
};
#endif //LOCALTHESIS_BUCKETMESSAGE_H
