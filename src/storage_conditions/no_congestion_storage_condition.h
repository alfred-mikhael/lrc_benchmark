#pragma once

#include "storage_conditions.h"

class NoCongestionStorageCondition : public StorageConditions
{
public:
    using StorageConditions::StorageConditions;

    double get_retrieval_delay(size_t machine_index, size_t data_size) const override
    {
        return get_baseline_retrieval_delay(machine_index, data_size);
    }
};
