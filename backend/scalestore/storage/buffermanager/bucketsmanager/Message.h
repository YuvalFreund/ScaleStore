//
// Created by YuvalFreund on 28.11.23.
//

#ifndef LOCALTHESIS_MESSAGE_H
#define LOCALTHESIS_MESSAGE_H

#include <cstddef>
#include <cstdint>
#include "MessagesEnum.h"

//TODO DFD
#define MESSAGE_SIZE 64
#define MSG_ENUM_IDX 0
#define MSG_SND_IDX 1
#define MSG_RCV_IDX 2
#define MSG_DATA_START_IDX 4
#define CONSENSUS_VEC_SIZE 3
#define BUCKET_ID_START_INDEX 16
#define PAGE_ID_START_INDEX 24
#define SSD_SLOT_START_INDEX 32


struct Message {
    std::byte messageData[MESSAGE_SIZE];
    MessagesEnum messageEnum;
    uint64_t sendingNode;
    vector<pair<uint64_t,uint64_t>>* unionFindVec;

    explicit Message(std::byte *newMessageData) {
        for (int i = 0; i < MESSAGE_SIZE; i++) {
            this->messageData[i] = newMessageData[i];
        }
        messageEnum = static_cast<MessagesEnum>((int) messageData[MSG_ENUM_IDX]);
        sendingNode = static_cast<uint64_t>(messageData[MSG_SND_IDX]);
    }
};
#endif //LOCALTHESIS_MESSAGE_H
