//
// Created by YuvalFreund on 10.03.23.
//

#include "Bucket.h"

struct BucketsCompare
{
    bool operator()(const Bucket& lhs, const Bucket& rhs)
    {
        return lhs.freeSlots.size() < rhs.freeSlots.size();
    }
};



