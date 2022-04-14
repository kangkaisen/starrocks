// This file_builder is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "orc_builder.h"

#include "column/const_column.h"
#include "column/datum_tuple.h"
#include "exprs/expr.h"
#include "exprs/vectorized/column_ref.h"
#include "formats/csv/converter.h"
#include "orc/OrcFile.hh"
#include "orc/Type.hh"
#include "runtime/buffer_control_block.h"
#include "util/date_func.h"
#include "util/logging.h"
#include "util/mysql_row_buffer.h"

namespace starrocks {

ORCOutputStream::ORCOutputStream(std::unique_ptr<WritableFile> writable_file) {
    _writable_file = std::move(writable_file);
}
ORCOutputStream::~ORCOutputStream() {
    LOG(WARNING) << "exit ORCOutputStream";
}

uint64_t ORCOutputStream::getLength() const {
    return _writable_file->size();
}
uint64_t ORCOutputStream::getNaturalWriteSize() const {
    return _writable_file->size();
}
void ORCOutputStream::write(const void* buf, size_t length) {
    Slice value((const uint8_t*)buf, length);
    _writable_file->append(value);
}
const std::string& ORCOutputStream::getName() const {
    return _writable_file->filename();
}
void ORCOutputStream::close() {
    _writable_file->close();
}

void fillLongValues(ColumnPtr& column, orc::ColumnVectorBatch* batch, uint64_t numValues, uint64_t read_start_idx) {
    auto* longBatch = dynamic_cast<orc::LongVectorBatch*>(batch);
    bool hasNull = false;
    for (uint64_t i = 0; i < numValues; ++i) {
        auto idx = read_start_idx + i;
        auto value = column->get(idx);
        if (value.is_null()) {
            batch->notNull[i] = 0;
            hasNull = true;
        } else {
            batch->notNull[i] = 1;
            longBatch->data[i] = value.get_int64();
        }
    }
    longBatch->hasNulls = hasNull;
    longBatch->numElements = numValues;
}

void fillStringValues(ColumnPtr& column, orc::ColumnVectorBatch* batch, uint64_t numValues,
                      orc::DataBuffer<char>& buffer, uint64_t read_start_idx) {
    uint64_t offset = 0;
    auto* stringBatch = dynamic_cast<orc::StringVectorBatch*>(batch);
    bool hasNull = false;
    for (uint64_t i = 0; i < numValues; ++i) {
        auto idx = read_start_idx + i;
        auto value = column->get(idx);
        if (value.is_null()) {
            batch->notNull[i] = 0;
            hasNull = true;
        } else {
            string col = value.get_slice().to_string();
            batch->notNull[i] = 1;
            char* oldBufferAddress = buffer.data();
            // Resize the buffer in case buffer does not have remaining space to store the next string.
            while (buffer.size() - offset < col.size()) {
                buffer.resize(buffer.size() * 2);
            }
            char* newBufferAddress = buffer.data();
            // Refill stringBatch->data with the new addresses, if buffer's address has changed.
            if (newBufferAddress != oldBufferAddress) {
                for (uint64_t refillIndex = 0; refillIndex < i; ++refillIndex) {
                    stringBatch->data[refillIndex] =
                            stringBatch->data[refillIndex] - oldBufferAddress + newBufferAddress;
                }
            }
            memcpy(buffer.data() + offset, col.c_str(), col.size());
            stringBatch->data[i] = buffer.data() + offset;
            stringBatch->length[i] = static_cast<int64_t>(col.size());
            offset += col.size();
        }
    }
    stringBatch->hasNulls = hasNull;
    stringBatch->numElements = numValues;
}

void fillDoubleValues(ColumnPtr& column, orc::ColumnVectorBatch* batch, uint64_t numValues, uint64_t read_start_idx) {
    auto* dblBatch = dynamic_cast<orc::DoubleVectorBatch*>(batch);
    bool hasNull = false;
    for (uint64_t i = 0; i < numValues; ++i) {
        auto idx = read_start_idx + i;
        auto value = column->get(idx);

        if (value.is_null()) {
            batch->notNull[i] = 0;
            hasNull = true;
        } else {
            batch->notNull[i] = 1;
            dblBatch->data[i] = value.get_double();
        }
    }
    dblBatch->hasNulls = hasNull;
    dblBatch->numElements = numValues;
}

// parse fixed point decimal numbers
void fillDecimalValues(ColumnPtr& column, orc::ColumnVectorBatch* batch, uint64_t numValues, size_t scale,
                       size_t precision, uint64_t read_start_idx) {
    orc::Decimal128VectorBatch* d128Batch = ORC_NULLPTR;
    orc::Decimal64VectorBatch* d64Batch = ORC_NULLPTR;
    if (precision <= 18) {
        d64Batch = dynamic_cast<orc::Decimal64VectorBatch*>(batch);
        d64Batch->scale = static_cast<int32_t>(scale);
    } else {
        d128Batch = dynamic_cast<orc::Decimal128VectorBatch*>(batch);
        d128Batch->scale = static_cast<int32_t>(scale);
    }
    bool hasNull = false;
    for (uint64_t i = 0; i < numValues; ++i) {
        auto idx = read_start_idx + i;
        auto value = column->get(idx);
        if (value.is_null()) {
            batch->notNull[i] = 0;
            hasNull = true;
        } else {
            auto slice = value.get_slice();
            string col(slice.get_data(), slice.get_size());
            batch->notNull[i] = 1;
            size_t ptPos = col.find('.');
            size_t curScale = 0;
            std::string num = col;
            if (ptPos != std::string::npos) {
                curScale = col.length() - ptPos - 1;
                num = col.substr(0, ptPos) + col.substr(ptPos + 1);
            }
            orc::Int128 decimal(num);
            while (curScale != scale) {
                curScale++;
                decimal *= 10;
            }
            if (precision <= 18) {
                d64Batch->values[i] = decimal.toLong();
            } else {
                d128Batch->values[i] = decimal;
            }
        }
    }
    batch->hasNulls = hasNull;
    batch->numElements = numValues;
}

void fillBoolValues(ColumnPtr& column, orc::ColumnVectorBatch* batch, uint64_t numValues, uint64_t read_start_idx) {
    auto* boolBatch = dynamic_cast<orc::LongVectorBatch*>(batch);
    bool hasNull = false;
    for (uint64_t i = 0; i < numValues; ++i) {
        auto idx = read_start_idx + i;
        auto value = column->get(idx);

        if (value.is_null()) {
            batch->notNull[i] = 0;
            hasNull = true;
        } else {
            batch->notNull[i] = 1;
            boolBatch->data[i] = value.get_int64();
        }
    }
    boolBatch->hasNulls = hasNull;
    boolBatch->numElements = numValues;
}

// parse date string from format YYYY-mm-dd
void fillDateValues(ColumnPtr& column, orc::ColumnVectorBatch* batch, uint64_t numValues, uint64_t read_start_idx) {
    auto* longBatch = dynamic_cast<orc::LongVectorBatch*>(batch);
    bool hasNull = false;
    auto const& t1970 = vectorized::DateValue::create(1970, 1, 1);
    for (uint64_t i = 0; i < numValues; ++i) {
        auto idx = read_start_idx + i;
        auto value = column->get(idx);

        if (value.is_null()) {
            batch->notNull[i] = 0;
            hasNull = true;
        } else {
            batch->notNull[i] = 1;
            double days = value.get_date().julian() - t1970.julian();
            longBatch->data[i] = static_cast<int64_t>(days);
        }
    }
    longBatch->hasNulls = hasNull;
    longBatch->numElements = numValues;
}

// parse timestamp values in seconds
void fillTimestampValues(ColumnPtr& column, orc::ColumnVectorBatch* batch, uint64_t numValues,
                         uint64_t read_start_idx) {
    auto* tsBatch = dynamic_cast<orc::TimestampVectorBatch*>(batch);
    bool hasNull = false;
    for (uint64_t i = 0; i < numValues; ++i) {
        auto idx = read_start_idx + i;
        auto value = column->get(idx);

        if (value.is_null()) {
            batch->notNull[i] = 0;
            hasNull = true;
        } else {
            if (value.is_null()) {
                batch->notNull[i] = 0;
            } else {
                batch->notNull[i] = 1;
                auto ts = value.get_timestamp();
                tsBatch->data[i] = ts.to_unix_second();
                tsBatch->nanoseconds[i] = 0;
            }
        }
    }
    tsBatch->hasNulls = hasNull;
    tsBatch->numElements = numValues;
}

const static std::unordered_map<PrimitiveType, orc::TypeKind> g_starrocks_orc_type_mapping = {
        {TYPE_BOOLEAN, orc::BOOLEAN}, {TYPE_TINYINT, orc::BYTE},
        {TYPE_SMALLINT, orc::SHORT},  {TYPE_INT, orc::INT},
        {TYPE_BIGINT, orc::LONG},     {TYPE_FLOAT, orc::FLOAT},
        {TYPE_DOUBLE, orc::DOUBLE},   {TYPE_DECIMALV2, orc::DECIMAL},
        {TYPE_DATE, orc::DATE},       {TYPE_DATETIME, orc::TIMESTAMP},
        {TYPE_VARCHAR, orc::STRING},  {TYPE_VARCHAR, orc::BINARY},
        {TYPE_CHAR, orc::CHAR},       {TYPE_VARCHAR, orc::VARCHAR},
};

static Status _starrocks_type_to_orc_type_descriptor(const TypeDescriptor& starrocks_type,
                                                     std::unique_ptr<orc::Type>& result) {
    if (starrocks_type.type == TYPE_ARRAY) {
        return Status::NotSupported("Unsupported StarRocks type: " + starrocks_type.debug_string());
    }

    auto precision = (int)starrocks_type.precision;
    if (precision < 0) {
        precision = starrocks_type.MAX_PRECISION;
    }
    auto scale = (int)starrocks_type.scale;
    if (scale < 0) {
        scale = starrocks_type.MAX_SCALE;
    }
    auto len = (int)starrocks_type.len;
    auto iter = g_starrocks_orc_type_mapping.find(starrocks_type.type);
    if (iter == g_starrocks_orc_type_mapping.end()) {
        return Status::NotSupported("Unsupported ORC type: " + starrocks_type.debug_string());
    }
    auto type = iter->second;
    switch (starrocks_type.type) {
    case TYPE_CHAR: {
        if (len < 0) {
            len = starrocks_type.MAX_CHAR_LENGTH;
        }
        result = orc::createCharType(orc::CHAR, len);
        break;
    }
    case TYPE_VARCHAR: {
        if (len < 0) {
            len = starrocks_type.MAX_VARCHAR_LENGTH;
        }
        result = orc::createCharType(orc::VARCHAR, len);
        break;
    }
    case TYPE_DECIMAL128:
    case TYPE_DECIMAL64:
    case TYPE_DECIMAL32:
    case TYPE_DECIMALV2: {
        result = orc::createDecimalType(precision, scale);
        break;
    }
    default: {
        result = orc::createPrimitiveType(type);
    }
    }

    return Status::OK();
}

ORCBuilder::ORCBuilder(ORCBuilderOptions options, std::unique_ptr<WritableFile> writable_file,
                       const std::vector<ExprContext*>& output_expr_ctxs)
        : _options(std::move(options)),
          _output_expr_ctxs(output_expr_ctxs),
          _batchSize(1024),
          _init(false),
          _dataBufferOffset(0),
          _writable_file(std::move(writable_file)) {}

Status ORCBuilder::init(vectorized::Chunk* chunk) {
    if (_init) {
        return Status::OK();
    }

    std::string schema_str = "struct<";
    bool is_first_field = true;
    size_t num_fields = _output_expr_ctxs.size();
    for (int idx = 0; idx < num_fields; idx++) {
        if (!is_first_field) {
            schema_str += ",";
        }
        const auto type = _output_expr_ctxs.at(idx)->root()->type();
        std::unique_ptr<orc::Type> result;
        Status status = _starrocks_type_to_orc_type_descriptor(type, result);
        if (!status.ok()) {
            return status;
        }
        auto root = _output_expr_ctxs.at(idx)->root();
        auto column_id = root->get_column_ref()->slot_id();
        auto column_name = chunk->get_column_name(column_id);
        if (column_name.size() > 0) {
            schema_str += string(column_name) + ":" + result->toString();
        } else {
            schema_str += "col" + std::to_string(idx) + ":" + result->toString();
        }
        is_first_field = false;
    }
    schema_str += ">";

    this->_type = orc::Type::buildTypeFromString(schema_str);
    _outStream = std::unique_ptr<orc::OutputStream>(new ORCOutputStream(std::move(_writable_file)));

    orc::MemoryPool* memoryPool = orc::getDefaultPool();
    orc::WriterOptions writerOptions;
    writerOptions.setStripeSize(16 * 1024);
    writerOptions.setCompressionBlockSize(1024);
    writerOptions.setCompression(orc::CompressionKind_ZLIB);
    writerOptions.setMemoryPool(memoryPool);
    writerOptions.setRowIndexStride(0);
    writerOptions.setFileVersion(orc::FileVersion::v_0_11());
    writerOptions.setTimezoneName("UTC");

    _writer = orc::createWriter(*this->_type, _outStream.get(), writerOptions);
    _rows = 0;

    _batch = _writer->createRowBatch(_batchSize);

    _init = true;

    return Status::OK();
}

Status ORCBuilder::add_chunk(vectorized::Chunk* chunk) {
    RETURN_IF_ERROR(init(chunk));

    const size_t num_rows = chunk->num_rows();
    size_t remain_rows = num_rows;
    size_t batch_write_rows;  // 一次循环写入行数
    size_t finished_rows = 0; // 已经完成写入行数
    size_t plan_rows = 0;     // 本轮循环完成后写入行数（用于下轮循环开始位置）

    while (remain_rows > 0) {
        if (remain_rows > _batchSize) {
            batch_write_rows = _batchSize;
            remain_rows -= _batchSize;
        } else {
            batch_write_rows = remain_rows;
            remain_rows = 0;
        }
        plan_rows += batch_write_rows;

        auto* structBatch = dynamic_cast<orc::StructVectorBatch*>(_batch.get());
        structBatch->numElements = batch_write_rows;
        DataBufferList bufferList;
        for (uint64_t i = 0; i < structBatch->fields.size(); ++i) {
            const orc::Type* subType = this->_type->getSubtype(i);
            auto columnData = chunk->get_column_by_index(i);
            switch (subType->getKind()) {
            case orc::BYTE:
            case orc::INT:
            case orc::SHORT:
            case orc::LONG:
                fillLongValues(columnData, structBatch->fields[i], batch_write_rows, finished_rows);
                break;
            case orc::STRING:
            case orc::CHAR:
            case orc::VARCHAR:
            case orc::BINARY:
                bufferList.emplace_back(*orc::getDefaultPool(), 1 * 1024 * 1024);
                fillStringValues(columnData, structBatch->fields[i], batch_write_rows, bufferList.back(),
                                 finished_rows);
                break;
            case orc::FLOAT:
            case orc::DOUBLE:
                fillDoubleValues(columnData, structBatch->fields[i], batch_write_rows, finished_rows);
                break;
            case orc::DECIMAL:
                fillDecimalValues(columnData, structBatch->fields[i], batch_write_rows, subType->getScale(),
                                  subType->getPrecision(), finished_rows);
                break;
            case orc::BOOLEAN:
                fillBoolValues(columnData, structBatch->fields[i], batch_write_rows, finished_rows);
                break;
            case orc::DATE:
                fillDateValues(columnData, structBatch->fields[i], batch_write_rows, finished_rows);
                break;
            case orc::TIMESTAMP:
            case orc::TIMESTAMP_INSTANT:
                fillTimestampValues(columnData, structBatch->fields[i], batch_write_rows, finished_rows);
                break;
            case orc::STRUCT:
            case orc::LIST:
            case orc::MAP:
            case orc::UNION:
                return Status::NotSupported(subType->toString() + " is not supported yet.");
            }
        }
        finished_rows = plan_rows;
        _writer->add(*_batch);
    }

    return Status::OK();
}

std::size_t ORCBuilder::file_size() {
    // return _writer->size();
    return 0;
}

Status ORCBuilder::finish() {
    _writer->close();

    return Status::OK();
}

} // namespace starrocks
