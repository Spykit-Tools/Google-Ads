/*MIT License

Copyright (c) 2024 spykit.tools

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.*/

/**
 * Основная функция для обработки CSV-файлов в заданной папке Google Drive.
 * Функция ищет CSV-файлы, не содержащие '__' в имени, затем удаляет первые 3 строки,
 * ищет уникальные значения во втором столбце, фильтрует данные по определенным критериям,
 * форматирует строки и создает новый CSV-файл с отфильтрованными данными.
 * После создания нового файла, исходный файл переименовывается и перемещается в корзину.
 */
function main() {
    var date = Utilities.formatDate(new Date(), 'GMT', 'yyyyMMdd');

    var folderCSV = DriveApp.getFolderById('*********************************');
    var files = folderCSV.getFilesByType(MimeType.CSV);
    while (files.hasNext()) {
        var file = files.next();
        var filename = file.getName();

        if (filename.indexOf('__') == -1) {
            var data = file.getBlob().getDataAsString();
            var arr = Utilities.parseCsv(data);
            arr.splice(0, 3);

            var uniqueArr = [];
            for (var i = 0; i < arr.length; i++) {
                uniqueArr.push([arr[i][1], 0]);
            }
            uniqueArr = unique(uniqueArr);
            var filterArr = [];

            for (var d = 0; d < uniqueArr.length; d++) {
                var line = uniqueArr[d];
                for (var l = 0; l < arr.length; l++) {
                    if (uniqueArr[d][0] == arr[l][1]) {
                        line[1] = +line[1] + +1;
                    }
                }
                if (line[1] >= 100) {
                    filterArr.push(line[0]);
                }
            }
            var filteredArr = [];
            for (var f = 0; f < filterArr.length; f++) {
                for (var l = 0; l < arr.length; l++) {
                    if (filterArr[f] == arr[l][1]) {
                        try {
                            arr[l][2] = Utilities.formatDate(new Date(arr[l][2]), "GMT", "yyyy-MM-dd'T'HH:mm:ss'Z'");
                            arr[l][3] = arr[l][3].replace(/\%/i, '').replace(/<10/i, '1');
                            arr[l][4] = arr[l][4].replace(/\%/i, '');
                            arr[l][5] = arr[l][5].replace(/\%/i, '');
                            arr[l][6] = arr[l][6].trim().replace(/\-\-/i, '0');
                            filteredArr.push(arr[l].join(','));
                        } catch (e) {
                            Logger.log(e);
                        }
                    }
                }
            }

            var newCSVstring = filteredArr.join('\r\n');

            var newFile = folderCSV.createFile('__AU_' + date + '.csv', newCSVstring);
            if (newFile) {
                try {
                    file.setName('___DEL__' + date + '.csv');
                    file.setTrashed(true);
                } catch (e) {
                    Logger.log(e);
                }
                var fileId = newFile.getId();

                loadCsv(fileId);
            }
        } else {
            try {
                file.setName('___DEL__' + date + '.csv');
                file.setTrashed(true);
            } catch (e) {
                Logger.log(e);
            }
        }
        break;
    }
}

/**
 * Фильтрует массив, удаляя из него повторяющиеся элементы.
 *
 * @param {Array} arr - Массив, который нужно отфильтровать.
 * @returns {Array} Новый массив, содержащий только уникальные элементы из исходного.
 */
function unique(arr) {
    var tmp = {};
    return arr.filter(function (a) {
        return a in tmp ? 0 : tmp[a] = 1;
    });
}

/**
 * Загружает CSV-файл в BigQuery, создавая новую таблицу и настраивая задание для загрузки данных.
 *
 * @param {string} csvFileId - Идентификатор файла CSV в Google Drive.
 */
function loadCsv(csvFileId) {
    var projectId = '***********'; // Project name
    var datasetId = '***********'; // dataset name
    var date = Utilities.formatDate(new Date(), 'GMT', 'yyyyMMdd');
    var tableId = 'AU_IS_' + date; // table name
    var table = {
        tableReference: {
            projectId: projectId,
            datasetId: datasetId,
            tableId: tableId
        },
        schema: {
            fields: [{
                    name: 'account', // if you do not have an MCC report, you will not need this field
                    type: 'STRING'
                },
                {
                    name: 'domain',
                    type: 'STRING'
                },
                {
                    name: 'keyword',
                    type: 'STRING'
                },
                {
                    name: 'date',
                    type: 'TIMESTAMP'
                },
                {
                    name: 'search_impr_share',
                    type: 'FLOAT'
                },
                {
                    name: 'top_of_page_rate',
                    type: 'FLOAT'
                },
                {
                    name: 'abs_top_of_page_rate',
                    type: 'FLOAT'
                },
                {
                    name: 'position_above_rate',
                    type: 'FLOAT'
                }
            ]
        }
    };
    table = BigQuery.Tables.insert(table, projectId, datasetId);
    Logger.log('Table created: %s', table.id);

    var file = DriveApp.getFileById(csvFileId);
    var data = file.getBlob().setContentType('application/octet-stream');
    // Create a job to load data
    var job = {
        configuration: {
            load: {
                destinationTable: {
                    projectId: projectId,
                    datasetId: datasetId,
                    tableId: tableId
                },
                skipLeadingRows: 1
            }
        }
    };
    job = BigQuery.Jobs.insert(job, projectId, data);
    Logger.log('Load job started. Check on the status of it here: ' + 'https://bigquery.cloud.google.com/jobs/%s', projectId);
}
