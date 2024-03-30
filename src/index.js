const { parseQuery } = require('./queryParser');
const readCSV = require('./csvReader');

function evaluateCondition(row, clause) {
    const { field, operator, value } = clause;
    const numericValue = !isNaN(value) ? parseFloat(value) : value;
    switch (operator) {
        case '=': return row[field] === value;
        case '!=': return row[field] !== value;
        case '>': return row[field] > value;
        case '<': return row[field] < value;
        case '>=': return row[field] >= value;
        case '<=': return row[field] <= value;
        default: throw new Error(`Unsupported operator: ${operator}`);
    }
}

async function performLeftJoin(mainData, joinData, joinCondition, fields, mainTable) {
    return mainData.flatMap(mainRow => {
        return joinData
            .filter(joinRow => mainRow[joinCondition.left.split('.')[1]] === joinRow[joinCondition.right.split('.')[1]])
            .map(joinRow => {
                return fields.reduce((acc, field) => {
                    const [tableName, fieldName] = field.split('.');
                    acc[field] = tableName === mainTable ? mainRow[fieldName] : joinRow[fieldName];
                    return acc;
                }, {});
            });
    });
}

async function performRightJoin(mainData, joinData, joinCondition, fields, mainTable) {
    return joinData.flatMap(joinRow => {
        return mainData
            .filter(mainRow => mainRow[joinCondition.left.split('.')[1]] === joinRow[joinCondition.right.split('.')[1]])
            .map(mainRow => {
                return fields.reduce((acc, field) => {
                    const [tableName, fieldName] = field.split('.');
                    acc[field] = tableName === mainTable ? mainRow[fieldName] : joinRow[fieldName];
                    return acc;
                }, {});
            });
    });
}

async function executeSELECTQuery(query) {
    const { fields, table, whereClauses, joinTable, joinCondition, joinType } = parseQuery(query);
    let data = await readCSV(`${table}.csv`);

    // Apply JOIN clauses
    if (joinTable && joinCondition && joinType) {
        const joinData = await readCSV(`${joinTable}.csv`);
        switch (joinType) {
            case 'INNER':
                data = data.flatMap(mainRow => {
                    return joinData
                        .filter(joinRow => mainRow[joinCondition.left.split('.')[1]] === joinRow[joinCondition.right.split('.')[1]])
                        .map(joinRow => {
                            return fields.reduce((acc, field) => {
                                const [tableName, fieldName] = field.split('.');
                                acc[field] = tableName === table ? mainRow[fieldName] : joinRow[fieldName];
                                return acc;
                            }, {});
                        });
                });
                break;
            case 'LEFT':
                data = await performLeftJoin(data, joinData, joinCondition, fields, table);
                break;
            case 'RIGHT':
                data = await performRightJoin(data, joinData, joinCondition, fields, table);
                break;
            default:
                throw new Error(`Unsupported JOIN type: ${joinType}`);
        }
    }


    // Apply WHERE clause filtering
    // Apply WHERE clause filtering
    const filteredData = whereClauses.length > 0
        ? data.filter(row => whereClauses.every(clause => evaluateCondition(row, clause)))
        : data;



    // Select the specified fields
    return filteredData.map(row => {
        const selectedRow = {};
        fields.forEach(field => {
            // Assuming 'field' is just the column name without table prefix
            selectedRow[field] = row[field];
        });
        return selectedRow;
    });
}

module.exports = executeSELECTQuery;



// Perform INNER JOIN if specified




