const fs = require('fs');
const path = require('path');
const csv = require('csv-parser');
const XLSX = require('xlsx');
const jschardet = require('jschardet');
const iconv = require('iconv-lite');

// 增强的编码检测功能
function detectFileEncoding(filePath) {
    try {
        // 读取更大的样本以提高准确性（从4096增加到8192字节）
        const buffer = fs.readFileSync(filePath);
        const result = jschardet.detect(buffer);

        // 更严格的编码检测规则
        if (!result || result.confidence < 0.85) {
            // 尝试检测 BOM 标记
            if (buffer.length >= 3) {
                if (buffer[0] === 0xEF && buffer[1] === 0xBB && buffer[2] === 0xBF) {
                    console.log(`检测到 UTF-8 BOM: ${path.basename(filePath)}`);
                    return 'utf8';
                }
                if (buffer[0] === 0xFE && buffer[1] === 0xFF) {
                    console.log(`检测到 UTF-16BE BOM: ${path.basename(filePath)}`);
                    return 'utf16be';
                }
                if (buffer[0] === 0xFF && buffer[1] === 0xFE) {
                    console.log(`检测到 UTF-16LE BOM: ${path.basename(filePath)}`);
                    return 'utf16le';
                }
            }

            // 如果没有 BOM，进行更细致的编码推断
            const testEncodings = ['utf8', 'gbk', 'big5', 'shift-jis'];
            for (const enc of testEncodings) {
                try {
                    const decoded = iconv.decode(buffer, enc);
                    const reEncoded = iconv.encode(decoded, enc);
                    if (buffer.equals(reEncoded)) {
                        console.log(`通过编码验证检测到: ${enc} (${path.basename(filePath)})`);
                        return enc;
                    }
                } catch (e) {
                    continue;
                }
            }

            console.log(`编码检测不确定，使用 UTF-8: ${path.basename(filePath)}`);
            return 'utf8';
        }

        // 更细致的编码名称标准化
        let encoding = result.encoding.toLowerCase();

        // 扩展的编码映射
        const encodingMap = {
            'gb2312': 'gbk',
            'gb18030': 'gbk',
            'windows-1252': 'cp1252',
            'iso-8859-1': 'latin1',
            'ascii': 'utf8',
            'utf-16le': 'utf16le',
            'utf-16be': 'utf16be'
        };

        encoding = encodingMap[encoding] || encoding;

        // 处理常见的中文编码
        if (encoding.includes('gb') || encoding.includes('gbk')) {
            encoding = 'gbk';
        } else if (encoding.includes('utf')) {
            encoding = encoding.replace('-', '').toLowerCase();
        }

        console.log(`检测到文件编码: ${encoding} (${path.basename(filePath)}), 置信度: ${result.confidence}`);
        return encoding;
    } catch (error) {
        console.error('编码检测失败:', error.message);
        return 'utf8';
    }
}

// 增强的 CSV 读取和解码功能
function readAndDecodeCSV(filePath, encoding) {
    try {
        const buffer = fs.readFileSync(filePath);
        let content;

        // 首先尝试使用检测到的编码
        try {
            content = iconv.decode(buffer, encoding);
        } catch (e) {
            console.warn(`使用 ${encoding} 解码失败，尝试备选编码...`);
            // 备选编码列表
            const fallbackEncodings = ['utf8', 'gbk', 'big5', 'shift-jis', 'cp1252'];
            for (const fallbackEncoding of fallbackEncodings) {
                if (fallbackEncoding === encoding) continue;
                try {
                    content = iconv.decode(buffer, fallbackEncoding);
                    console.log(`成功使用备选编码 ${fallbackEncoding} 解码`);
                    return content;
                } catch (err) {
                    continue;
                }
            }
            throw new Error('所有编码尝试均失败');
        }

        // 检查解码后的内容是否包含明显的乱码特征
        const invalidChars = content.match(/[\uFFFD\u0000-\u0008\u000B-\u000C\u000E-\u001F]/g);
        if (invalidChars && invalidChars.length > content.length * 0.1) {
            console.warn('检测到可能的乱码，尝试其他编码...');
            // 尝试其他编码...
            const alternativeEncodings = ['gbk', 'big5', 'shift-jis'];
            for (const altEncoding of alternativeEncodings) {
                try {
                    const altContent = iconv.decode(buffer, altEncoding);
                    const altInvalidChars = altContent.match(/[\uFFFD\u0000-\u0008\u000B-\u000C\u000E-\u001F]/g);
                    if (!altInvalidChars || altInvalidChars.length < invalidChars.length) {
                        console.log(`使用替代编码 ${altEncoding} 得到更好的结果`);
                        return altContent;
                    }
                } catch (err) {
                    continue;
                }
            }
        }

        return content;
    } catch (error) {
        throw new Error(`文件读取或解码失败: ${error.message}`);
    }
}

async function convertCsvToExcel(inputCsvPath, outputExcelPath) {
    try {
        const results = [];

        // 检测文件编码
        const encoding = detectFileEncoding(inputCsvPath);

        // 读取并解码 CSV 内容
        const csvContent = readAndDecodeCSV(inputCsvPath, encoding);

        // 检查 CSV 内容的有效性
        if (!csvContent || csvContent.trim().length === 0) {
            throw new Error('CSV 内容为空或无效');
        }

        await new Promise((resolve, reject) => {
            const Readable = require('stream').Readable;
            const s = new Readable();
            s._read = () => { };
            s.push(csvContent);
            s.push(null);

            s.pipe(csv({
                separator: ',', // 自动检测分隔符
                headers: true,
                skipLines: 0,
                strict: true,
                mapValues: ({ header, value }) => {
                    if (value === '') return null;

                    // 清理值中的不可见字符
                    value = value.replace(/[\x00-\x1F\x7F]/g, '').trim();

                    // 尝试转换数字
                    if (/^-?\d*\.?\d+$/.test(value)) {
                        const num = Number(value);
                        if (!isNaN(num)) return num;
                    }

                    // 尝试转换日期（支持多种格式）
                    const dateFormats = [
                        /^\d{4}[-/]\d{1,2}[-/]\d{1,2}$/,
                        /^\d{1,2}[-/]\d{1,2}[-/]\d{4}$/,
                        /^\d{4}年\d{1,2}月\d{1,2}日$/
                    ];

                    if (dateFormats.some(format => format.test(value))) {
                        const date = new Date(value);
                        if (!isNaN(date)) return date;
                    }

                    return value;
                }
            }))
                .on('data', (data) => results.push(data))
                .on('end', () => resolve())
                .on('error', (error) => reject(error));
        });

        // 创建工作簿并设置属性
        const workbook = XLSX.utils.book_new();
        workbook.Props = {
            Title: path.basename(inputCsvPath, '.csv'),
            CreatedDate: new Date()
        };

        // 将数据转换为工作表
        const worksheet = XLSX.utils.json_to_sheet(results, {
            dateNF: 'yyyy-mm-dd'
        });

        // 优化列宽设置
        const columnWidths = {};
        const maxSampleRows = Math.min(100, results.length); // 采样前100行来设置列宽

        // 处理表头
        Object.keys(results[0] || {}).forEach(key => {
            columnWidths[key] = key.length;
        });

        // 处理数据行
        results.slice(0, maxSampleRows).forEach(row => {
            Object.entries(row).forEach(([key, value]) => {
                const cellWidth = String(value || '').length;
                columnWidths[key] = Math.max(
                    columnWidths[key] || 0,
                    Math.min(cellWidth, 50) // 限制单个单元格的最大宽度
                );
            });
        });

        worksheet['!cols'] = Object.values(columnWidths).map(width => ({
            wch: Math.min(width + 2, 50) // 添加一些内边距，并限制最大宽度
        }));

        // 添加工作表到工作簿
        XLSX.utils.book_append_sheet(workbook, worksheet, 'Sheet1');

        // 写入 Excel 文件
        XLSX.writeFile(workbook, outputExcelPath);

        console.log(`转换完成: ${path.basename(inputCsvPath)} -> ${path.basename(outputExcelPath)} (${encoding})`);
        return true;
    } catch (error) {
        console.error(`转换失败 ${path.basename(inputCsvPath)}:`, error.message);
        return false;
    }
}

async function processDirectory(inputDir, outputDir) {
    try {
        // 确保输出目录存在
        if (!fs.existsSync(outputDir)) {
            fs.mkdirSync(outputDir, { recursive: true });
        }

        // 读取目录中的所有文件
        const files = fs.readdirSync(inputDir);
        const csvFiles = files.filter(file => file.toLowerCase().endsWith('.csv'));

        if (csvFiles.length === 0) {
            console.log('未找到CSV文件');
            return;
        }

        console.log(`找到 ${csvFiles.length} 个CSV文件`);
        console.log('------------------------');

        const results = {
            success: 0,
            failed: 0,
            total: csvFiles.length,
            failedFiles: []
        };

        // 转换所有CSV文件
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

        // 输出详细的转换统计
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
    const inputDir = args[0] || './data';
    const outputDir = args[1] || './excel_files';

    console.log('开始转换...');
    console.log(`输入目录: ${inputDir}`);
    console.log(`输出目录: ${outputDir}`);
    console.log('------------------------');

    processDirectory(inputDir, outputDir);
}