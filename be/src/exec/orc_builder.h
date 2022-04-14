// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include <cstdint>
#include <map>
#include <string>
#include <utility>

#include "column/datum_tuple.h"
#include "exec/file_builder.h"
#include "formats/csv/converter.h"
#include "orc/OrcFile.hh"

namespace starrocks {

namespace vectorized::csv {
class Converter;
class OutputStream;
} // namespace vectorized::csv

class ExprContext;
class FileWriter;

struct ORCBuilderOptions {
    std::string column_terminated_by;
    std::string line_terminated_by;
};

class ORCOutputStream : public orc::OutputStream {
public:
    ORCOutputStream(std::unique_ptr<WritableFile> writable_file);
    ~ORCOutputStream() override;

    uint64_t getLength() const override;
    uint64_t getNaturalWriteSize() const override;
    void write(const void* buf, size_t length) override;
    const std::string& getName() const override;
    void close() override;

private:
    std::unique_ptr<WritableFile> _writable_file;
};

typedef std::list<orc::DataBuffer<char>> DataBufferList;

class ORCBuilder final : public FileBuilder {
public:
    ORCBuilder(ORCBuilderOptions options, std::unique_ptr<WritableFile> writable_file,
               const std::vector<ExprContext*>& output_expr_ctxs);
    ~ORCBuilder() override = default;

    Status add_chunk(vectorized::Chunk* chunk) override;

    std::size_t file_size() override;

    Status finish() override;

private:
    const static size_t OUTSTREAM_BUFFER_SIZE_BYTES;
    const ORCBuilderOptions _options;
    const std::vector<ExprContext*>& _output_expr_ctxs;
    ORC_UNIQUE_PTR<orc::Writer> _writer;
    std::unique_ptr<orc::Type> _type;
    std::unique_ptr<orc::ColumnVectorBatch> _batch;
    std::unique_ptr<orc::OutputStream> _outStream;
    uint64_t _batchSize;
    bool _init;
    uint64_t _rows = 0;
    uint64_t _dataBufferOffset = 0;
    std::unique_ptr<WritableFile> _writable_file;

    Status init(vectorized::Chunk* chunk);
};

} // namespace starrocks
