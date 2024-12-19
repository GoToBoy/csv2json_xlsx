const fs = require('fs');
const path = require('path');
const { parse } = require('csv-parse');
const XLSX = require('xlsx');
const jschardet = require('jschardet');
const iconv = require('iconv-lite');

// 配置选项
const config = {
    csvOptions: {
        delimiter: ',',
        columns: true,
        skip_empty_lines: true,
        trim: true,
        bom: true,
        quote: '"',
        escape: '"',
        relax_quotes: true,
        relax_column_count: true,
        relax: true,
        skip_records_with_error: true
    },
    encodings: ['utf8', 'gbk', 'gb2312', 'big5'],
    // 添加允许的病人ID列表
    allowedPatientIds: [
        'PA100',
    ]
};

function detectFileEncoding(filePath) {
    try {
        const buffer = fs.readFileSync(filePath);
        const result = jschardet.detect(buffer);

        if (!result || result.confidence < 0.8) {
            console.log(`编码检测可信度低，默认使用 GBK: ${path.basename(filePath)}`);
            return 'gbk';
        }

        let encoding = result.encoding.toLowerCase();

        if (encoding === 'ascii' || encoding === 'utf-8') {
            return 'utf8';
        } else if (encoding.includes('gb') || encoding === 'gbk') {
            return 'gbk';
        } else if (encoding === 'big5') {
            return 'big5';
        }

        console.log(`检测到文件编码: ${encoding} (${path.basename(filePath)})`);
        return encoding;
    } catch (error) {
        console.error('编码检测失败:', error.message);
        return 'gbk';
    }
}

function readAndDecodeCSV(filePath, encoding) {
    try {
        const buffer = fs.readFileSync(filePath);
        let content = iconv.decode(buffer, encoding);

        content = content
            .replace(/\r\n/g, '\n')
            .replace(/\u0000/g, '')
            .replace(/[\x00-\x09\x0B-\x1F\x7F]/g, '');

        return content;
    } catch (error) {
        throw new Error(`文件读取或解码失败: ${error.message}`);
    }
}

// 修改后的 parseCSVContent 函数，增加病人ID过滤
function parseCSVContent(content) {
    return new Promise((resolve, reject) => {
        parse(content, {
            ...config.csvOptions,
            on_record: (record, context) => {
                // 清理记录
                Object.keys(record).forEach(key => {
                    if (typeof record[key] === 'string') {
                        record[key] = record[key]
                            .replace(/[\x00-\x09\x0B-\x1F\x7F]/g, '')
                            .trim();
                    }
                });

                // 检查是否包含 PATIENT_ID 字段并且在允许列表中
                if (record.PATIENT_ID && config.allowedPatientIds.includes(record.PATIENT_ID)) {
                    return record;
                }
                // 如果不在允许列表中，返回 null（这条记录会被过滤掉）
                return null;
            }
        }, (err, records) => {
            if (err) reject(err);
            else {
                // 过滤掉 null 记录
                const filteredRecords = records.filter(record => record !== null);
                resolve(filteredRecords);
            }
        });
    });
}

async function convertCsvToExcel(inputCsvPath, outputExcelPath) {
    try {
        const encoding = detectFileEncoding(inputCsvPath);
        const csvContent = readAndDecodeCSV(inputCsvPath, encoding);
        const records = await parseCSVContent(csvContent);

        // 过滤掉空记录
        const validRecords = records.filter(record =>
            Object.values(record).some(value => value !== null && value !== '')
        );

        if (validRecords.length === 0) {
            throw new Error('没有符合条件的记录');
        }

        const workbook = XLSX.utils.book_new();
        workbook.Props = {
            Title: path.basename(inputCsvPath, '.csv'),
            CreatedDate: new Date()
        };

        const worksheet = XLSX.utils.json_to_sheet(validRecords, {
            dateNF: 'yyyy-mm-dd'
        });

        // 优化列宽设置
        const columnWidths = {};
        const maxSampleRows = Math.min(100, validRecords.length);

        Object.keys(validRecords[0] || {}).forEach(key => {
            columnWidths[key] = key.length;
        });

        validRecords.slice(0, maxSampleRows).forEach(row => {
            Object.entries(row).forEach(([key, value]) => {
                const cellWidth = String(value || '').length;
                columnWidths[key] = Math.max(
                    columnWidths[key] || 0,
                    Math.min(cellWidth, 50)
                );
            });
        });

        worksheet['!cols'] = Object.values(columnWidths).map(width => ({
            wch: Math.min(width + 2, 50)
        }));

        XLSX.utils.book_append_sheet(workbook, worksheet, 'Sheet1');
        XLSX.writeFile(workbook, outputExcelPath);

        console.log(`转换完成: ${path.basename(inputCsvPath)} -> ${path.basename(outputExcelPath)} (${encoding})`);
        console.log(`- 总记录数: ${records.length}`);
        console.log(`- 符合条件的记录数: ${validRecords.length}`);
        console.log(`- 已过滤的病人ID: ${config.allowedPatientIds.join(', ')}`);
        return true;
    } catch (error) {
        console.error(`转换失败 ${path.basename(inputCsvPath)}:`, error.message);
        console.error('详细错误:', error);
        return false;
    }
}

async function processDirectory(inputDir, outputDir) {
    try {
        if (!fs.existsSync(outputDir)) {
            fs.mkdirSync(outputDir, { recursive: true });
        }

        const files = fs.readdirSync(inputDir);
        const csvFiles = files.filter(file => file.toLowerCase().endsWith('.csv'));

        if (csvFiles.length === 0) {
            console.log('未找到CSV文件');
            return;
        }

        console.log(`找到 ${csvFiles.length} 个CSV文件`);
        console.log('------------------------');
        console.log('仅处理以下病人ID的记录:');
        config.allowedPatientIds.forEach(id => console.log(`- ${id}`));
        console.log('------------------------');

        const results = {
            success: 0,
            failed: 0,
            total: csvFiles.length,
            failedFiles: []
        };

        for (const csvFile of csvFiles) {
            const inputPath = path.join(inputDir, csvFile);
            const outputPath = path.join(
                outputDir,
                `${path.basename(csvFile, '.csv')}.xlsx`
            );

            console.log(`正在处理: ${csvFile}`);
            const success = await convertCsvToExcel(inputPath, outputPath);

            if (success) {
                results.success++;
            } else {
                results.failed++;
                results.failedFiles.push(csvFile);
            }
        }

        console.log('\n转换统计:');
        console.log(`总文件数: ${results.total}`);
        console.log(`成功: ${results.success}`);
        console.log(`失败: ${results.failed}`);
        console.log(`完成率: ${((results.success / results.total) * 100).toFixed(2)}%`);

        if (results.failedFiles.length > 0) {
            console.log('\n失败的文件:');
            results.failedFiles.forEach(file => console.log(`- ${file}`));
        }

        console.log(`\n输出目录: ${outputDir}`);

    } catch (error) {
        console.error('处理过程中发生错误:', error.message);
    }
}

// 命令行支持
if (require.main === module) {
    const args = process.argv.slice(2);
    const inputDir = args[0] || './data_10';
    const outputDir = args[1] || './excel_files_10_filtered';

    console.log('开始转换...');
    console.log(`输入目录: ${inputDir}`);
    console.log(`输出目录: ${outputDir}`);
    console.log('------------------------');

    processDirectory(inputDir, outputDir);
}