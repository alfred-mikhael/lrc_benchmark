#include "./recoverable.h"
#include <fstream>
#include <limits>
#include <stdexcept>

Recoverable::Recoverable(const std::filesystem::path &path, const LRC &code) : code(code)
{
    const size_t num_data_shards = code.num_data_shards();
    if (num_data_shards == 0)
        throw std::invalid_argument("An LRC must have at least one data shard");

    const auto file_size = std::filesystem::file_size(path);
    const auto max_size = std::numeric_limits<size_t>::max();
    if (file_size > max_size || file_size > std::numeric_limits<std::streamsize>::max())
        throw std::length_error("File is too large");

    data_size = static_cast<size_t>(file_size);
    const size_t shard_size = data_size / num_data_shards + (data_size % num_data_shards != 0);
    const size_t local_shards = code.num_local_shards();
    const size_t global_shards = code.num_global_shards();
    if (global_shards > max_size - local_shards)
        throw std::length_error("Too many parity shards");
    const size_t num_parity_shards = local_shards + global_shards;
    if (shard_size != 0 &&
        (num_data_shards > max_size / shard_size || num_parity_shards > max_size / shard_size))
        throw std::length_error("Shard buffers are too large");

    data = std::make_unique<std::byte[]>(shard_size * num_data_shards);
    parity_size = num_parity_shards * shard_size;
    paritiy = std::make_unique<std::byte[]>(parity_size);

    std::ifstream input;
    input.exceptions(std::ios::failbit | std::ios::badbit);
    input.open(path, std::ios::binary);
    if (data_size != 0)
        input.read(reinterpret_cast<char *>(data.get()), static_cast<std::streamsize>(data_size));
}

void Recoverable::write(const std::filesystem::path &path)
{
    Recoverable replacement(path, code);
    data.swap(replacement.data);
    paritiy.swap(replacement.paritiy);
    data_size = replacement.data_size;
    parity_size = replacement.parity_size;
}

void Recoverable::write(std::byte byte, size_t offset)
{
    write_byte(byte, offset);
    // TODO: Update parity to reflect the modified byte.
}

void Recoverable::write_byte(std::byte byte, size_t offset)
{
    if (offset >= data_size)
        throw std::out_of_range("Data offset is out of range");
    data[offset] = byte;
}

std::byte Recoverable::read(size_t offset)
{
    if (offset >= data_size)
        throw std::out_of_range("Data offset is out of range");
    return data[offset];
}

std::byte Recoverable::read_parity(size_t offset)
{
    if (offset >= parity_size)
        throw std::out_of_range("Parity offset is out of range");
    return paritiy[offset];
}
