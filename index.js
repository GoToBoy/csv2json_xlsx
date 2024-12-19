const fs = require('fs').promises;
const path = require('path');
const csv = require('csv-parse');
const { promisify } = require('util');
const iconv = require('iconv-lite');
const jschardet = require('jschardet');

const parseCSV = promisify(csv.parse);

// 配置选项
const config = {
    inputDir: './data',
    outputDir: './output',
    cleanOutputDir: true,
    csvOptions: {
        delimiter: ',',
        columns: true,
        skip_empty_lines: true,
        trim: true,
        bom: true,
        // 添加以下选项来处理复杂的引号情况
        quote: '"',
        escape: '"',
        relax_quotes: true,  // 放宽引号规则
        relax_column_count: true,  // 允许不一致的列数
        relax: true,  // 允许解析不规范的CSV
        skip_records_with_error: true  // 跳过有错误的记录而不是终止解析
    },
    encodings: ['utf8', 'gbk', 'gb2312', 'big5'],
    outputEncoding: 'utf8'
};

// 清空目录函数保持不变
async function cleanDirectory(dirPath) {
    try {
        const files = await fs.readdir(dirPath);
        const deletePromises = files.map(file => {
            const filePath = path.join(dirPath, file);
            return fs.unlink(filePath);
        });
        await Promise.all(deletePromises);
        console.log(`Cleaned output directory: ${dirPath}`);
    } catch (error) {
        if (error.code !== 'ENOENT') {
            throw error;
        }
    }
}

// 确保目录存在函数保持不变
async function ensureDir(dirPath, clean = false) {
    try {
        try {
            await fs.access(dirPath);
            if (clean) {
                await cleanDirectory(dirPath);
            }
        } catch {
            await fs.mkdir(dirPath, { recursive: true });
            console.log(`Created output directory: ${dirPath}`);
        }
    } catch (error) {
        console.error(`Error handling directory ${dirPath}:`, error.message);
        throw error;
    }
}

// 增强的文件编码检测
async function detectFileEncoding(filePath) {
    const buffer = await fs.readFile(filePath);
    const result = jschardet.detect(buffer);
    // 如果检测结果可信度低，默认使用 GBK
    if (result.confidence < 0.8) {
        console.log(`Low confidence encoding detection for ${path.basename(filePath)}, defaulting to GBK`);
        return 'gbk';
    }
    return result.encoding;
}

// 增强的CSV读取函数
async function readCSVWithEncoding(filePath) {
    try {
        const buffer = await fs.readFile(filePath);
        const detectedEncoding = (await detectFileEncoding(filePath)).toLowerCase();
        console.log(`Detected encoding for ${path.basename(filePath)}: ${detectedEncoding}`);

        let content;
        if (detectedEncoding === 'ascii' || detectedEncoding === 'utf-8') {
            content = buffer.toString('utf8');
        } else if (detectedEncoding.includes('gb') || detectedEncoding === 'gbk') {
            content = iconv.decode(buffer, 'gbk');
        } else if (detectedEncoding === 'big5') {
            content = iconv.decode(buffer, 'big5');
        } else {
            content = iconv.decode(buffer, 'utf8');
        }

        // 预处理内容，处理可能导致解析错误的字符
        content = content
            .replace(/\r\n/g, '\n')  // 统一换行符
            .replace(/\u0000/g, ''); // 移除 NULL 字符

        return content;
    } catch (error) {
        throw new Error(`Error reading file ${filePath}: ${error.message}`);
    }
}

// 增强的CSV转JSON函数
async function convertCSVToJSON(filePath) {
    try {
        const csvContent = await readCSVWithEncoding(filePath);

        // 使用更宽松的解析选项
        const records = await parseCSV(csvContent, {
            ...config.csvOptions,
            // 添加错误处理回调
            on_record: (record, context) => {
                // 清理每个字段中的问题字符
                Object.keys(record).forEach(key => {
                    if (typeof record[key] === 'string') {
                        record[key] = record[key]
                            .replace(/[\x00-\x09\x0B-\x1F\x7F]/g, '') // 移除控制字符
                            .trim();
                    }
                });
                return record;
            }
        });

        // 过滤掉空记录
        const validRecords = records.filter(record =>
            Object.values(record).some(value => value !== null && value !== '')
        );

        const fileName = path.basename(filePath, '.csv');
        const outputPath = path.join(config.outputDir, `${fileName}.json`);

        await fs.writeFile(
            outputPath,
            JSON.stringify(validRecords, null, 2, (key, value) => {
                if (typeof value === 'string') {
                    return value.normalize('NFC');
                }
                return value;
            }),
            'utf8'
        );

        console.log(`Successfully converted ${fileName}.csv to JSON`);
        console.log(`- Original records: ${records.length}`);
        console.log(`- Valid records: ${validRecords.length}`);
        return true;
    } catch (error) {
        console.error(`Error converting ${filePath}:`, error.message);
        // 记录更详细的错误信息
        console.error('Detailed error:', error);
        return false;
    }
}

// 主函数保持不变
async function convertAllCSVFiles() {
    try {
        console.log('Starting CSV to JSON conversion...');
        console.log(`Input directory: ${config.inputDir}`);
        console.log(`Output directory: ${config.outputDir}`);

        await ensureDir(config.outputDir, config.cleanOutputDir);
        await ensureDir(config.inputDir, false);

        const files = await fs.readdir(config.inputDir);
        const csvFiles = files.filter(file =>
            file.toLowerCase().endsWith('.csv')
        );

        if (csvFiles.length === 0) {
            console.log('No CSV files found in the input directory');
            return;
        }

        console.log(`Found ${csvFiles.length} CSV files to convert`);

        const results = await Promise.all(
            csvFiles.map(file =>
                convertCSVToJSON(path.join(config.inputDir, file))
            )
        );

        const successCount = results.filter(result => result).length;
        console.log(`
Conversion completed:
- Total files: ${csvFiles.length}
- Successfully converted: ${successCount}
- Failed: ${csvFiles.length - successCount}
        `);

    } catch (error) {
        console.error('Error processing files:', error.message);
        process.exit(1);
    }
}

// 运行程序
convertAllCSVFiles().catch(error => {
    console.error('Fatal error:', error);
    process.exit(1);
});