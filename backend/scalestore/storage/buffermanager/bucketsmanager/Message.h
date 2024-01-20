//
// Created by YuvalFreund on 28.11.23.
//

#ifndef LOCALTHESIS_MESSAGE_H
#define LOCALTHESIS_MESSAGE_H

#include <cstddef>
#include <cstdint>
#include "MessagesEnum.h"

#include "BucketManagerDefs.h"

struct Message {
    std::byte messageData[MESSAGE_SIZE];
    MessagesEnum messageEnum;

    explicit Message(std::byte *newMessageData) {
        for (int i = 0; i < MESSAGE_SIZE; i++) {
            this->messageData[i] = newMessageData[i];
        }
        messageEnum = static_cast<MessagesEnum>((int) messageData[MSG_ENUM_IDX]);
    }
};
#endif //LOCALTHESIS_MESSAGE_H
