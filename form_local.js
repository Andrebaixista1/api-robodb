import axios from 'axios';
import mysql2 from 'mysql2';
import moment from 'moment';
import { createClient } from '@supabase/supabase-js';

// Configuração do logging
const logging = {
    info: (message) => console.log(`INFO: ${message}`),
    error: (message) => console.error(`ERROR: ${message}`)
};

// Função para criar conexão com o MySQL
function createConnection() {
    return mysql2.createConnection({
        host: "26.101.42.111",
        user: "andrefelipe",
        password: "vieira@165",
        database: "himalia"
    });
}

// Função para upsert dados no MySQL
async function upsertDataPg(df) {
    const conn = createConnection();
    const upsertQuery = `
        INSERT INTO data_formalizacao (
            \`Banco Averbador\`, \`Status API\`, \`Data de Pagamento\`, \`Data de Formalizacao\`,
            \`Beneficio\`, \`Banco Refinanciador\`, \`Produto\`, \`ID Proposta Banco\`,
            \`Valor Financiado\`, \`Valor Parcela\`, \`Taxa Enquadrada\`, \`Pagas\`, \`Prazo\`, \`Equipe\`, \`Data Status API\`,
            \`Franquias\`, \`Status\`, \`Vendedor\`, \`Origem\`, \`CEP\`, \`CIDADE\`, \`UF\`, \`Empresa\`
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON DUPLICATE KEY UPDATE
            \`Data de Pagamento\` = VALUES(\`Data de Pagamento\`),
            \`Valor Parcela\` = VALUES(\`Valor Parcela\`),
            \`Taxa Enquadrada\` = VALUES(\`Taxa Enquadrada\`),
            \`Pagas\` = VALUES(\`Pagas\`),
            \`Prazo\` = VALUES(\`Prazo\`),
            \`Banco Averbador\` = VALUES(\`Banco Averbador\`),
            \`Status API\` = VALUES(\`Status API\`),
            \`Beneficio\` = VALUES(\`Beneficio\`),
            \`Banco Refinanciador\` = VALUES(\`Banco Refinanciador\`),
            \`Produto\` = VALUES(\`Produto\`),
            \`Valor Financiado\` = VALUES(\`Valor Financiado\`),
            \`Equipe\` = VALUES(\`Equipe\`),
            \`Data Status API\` = VALUES(\`Data Status API\`),
            \`Franquias\` = VALUES(\`Franquias\`),
            \`Status\` = VALUES(\`Status\`),
            \`Vendedor\` = VALUES(\`Vendedor\`),
            \`Origem\` = VALUES(\`Origem\`),
            \`CEP\` = VALUES(\`CEP\`),
            \`CIDADE\` = VALUES(\`CIDADE\`),
            \`UF\` = VALUES(\`UF\`),
            \`Empresa\` = VALUES(\`Empresa\`);
    `;

    try {
        for (const row of df) {
            const values = [
                row["Banco Averbador"],
                row["Status API"],
                row["Data de Pagamento"],
                row["Data de Formalizacao"],
                row["Beneficio"],
                row["Banco Refinanciador"],
                row["Produto"],
                row["ID Proposta Banco"],
                row["Valor Financiado"],
                row["Valor Parcela"],
                row["Taxa Enquadrada"],
                row["Pagas"],
                row["Prazo"],
                row["Equipe"],
                row["Data Status API"],
                row["Franquias"],
                row["Status"],
                row["Vendedor"],
                row["Origem"],
                row["CEP"],
                row["CIDADE"],
                row["UF"],
                row["Empresa"]
            ];
            await conn.promise().execute(upsertQuery, values);
        }
        logging.info("Dados inseridos/atualizados na tabela data_formalizacao com sucesso!");
    } catch (e) {
        logging.error(`Erro ao inserir/atualizar dados: ${e}`);
    } finally {
        conn.end();
    }
}

// Função para buscar dados da API
async function fetchDatapgApi(startDate, endDate) {
    const urls = [
        { empresa: "vieira", url: "https://api.newcorban.com.br/api/propostas/", username: "robo.planejamento", password: "Vieira@165" },
        { empresa: "abbcred", url: "https://api.newcorban.com.br/api/propostas/", username: "robo.planejamento", password: "Vieira@2024!" },
        { empresa: "impacto", url: "https://api.newcorban.com.br/api/propostas/", username: "planejamento", password: "Vieira@165" },
        { empresa: "franquiasazul", url: "https://api.newcorban.com.br/api/propostas/", username: "Azul.dev", password: "Vieira@165" },
        { empresa: "gmpromotora", url: "https://api.newcorban.com.br/api/propostas/", username: "GMPro995.master", password: "Expande03@@1" }
    ];

    const combinedData = [];
    for (const entry of urls) {
        const startDateStr = moment(startDate).format('YYYY-MM-DD');
        const endDateStr = moment(endDate).format('YYYY-MM-DD');

        const payload = {
            auth: {
                username: entry.username,
                password: entry.password,
                empresa: entry.empresa
            },
            requestType: "getPropostas",
            filters: {
                data: {
                    tipo: "data_formalizacao",
                    startDate: startDateStr,
                    endDate: endDateStr
                }
            }
        };

        try {
            const response = await axios.post(entry.url, payload, { headers: { "Content-Type": "application/json" } });
            if (response.status === 200) {
                const data = response.data;
                for (const key in data) {
                    if (data.hasOwnProperty(key) && typeof data[key] === 'object') {
                        const item = data[key];
                        combinedData.push({
                            "Banco Averbador": item.averbacao?.banco_averbacao || "NÃO INFORMADO",
                            "Status API": item.api?.status_api || "NÃO INFORMADO",
                            "Data de Pagamento": item.datas?.pagamento ? moment(item.datas.pagamento).format('YYYY-MM-DD') : null,
                            "Data de Formalizacao": item.datas?.data_formalizacao ? moment(item.datas.data_formalizacao).format('YYYY-MM-DD') : null,
                            "Beneficio": item.cliente?.matricula || null,
                            "Banco Refinanciador": item.proposta?.banco_nome || "NÃO INFORMADO",
                            "Produto": item.proposta?.produto_nome || "NÃO INFORMADO",
                            "ID Proposta Banco": item.proposta?.proposta_id_banco || "NÃO INFORMADO",
                            "Valor Financiado": item.proposta?.valor_referencia || 0.0,
                            "Valor Parcela": item.proposta?.valor_parcela || 0.0,
                            "Taxa Enquadrada": item.proposta?.taxa || 0.0,
                            "Pagas": item.contratos ? Object.values(item.contratos)[0]?.count_parcelas_pagas || 0.0 : 0.0,
                            "Prazo": item.proposta?.prazo || 0,
                            "Equipe": item.equipe_nome,
                            "Data Status API": item.data_status ? moment(item.data_status).format('YYYY-MM-DD') : "NÃO INFORMADO",
                            "Franquias": item.franquia_nome,
                            "Status": item.status_nome || "NÃO INFORMADO",
                            "Vendedor": item.vendedor_nome || "NÃO INFORMADO",
                            "Origem": item.origem || "NÃO INFORMADO",
                            "CEP": item.cliente?.endereco?.cep || null,
                            "CIDADE": item.cliente?.endereco?.cidade || null,
                            "UF": item.cliente?.endereco?.estado || null,
                            "Empresa": entry.empresa
                        });
                    }
                }
            } else {
                logging.info(`Status code diferente de 200 para ${entry.empresa}`);
            }
        } catch (e) {
            logging.error(`Erro ao acessar a API da empresa ${entry.empresa}: ${e}`);
        }
    }
    return combinedData;
}

// Função principal
async function main() {
    const today = moment().toDate();
    const currentYear = today.getFullYear();
    const currentMonth = today.getMonth() + 1;
    const lastDay = new Date(currentYear, currentMonth, 0).getDate();

    let startDate = new Date(currentYear, currentMonth - 1, 1);
    let endDate = new Date(startDate);
    endDate.setDate(startDate.getDate() + 14);

    if (endDate > today) {
        endDate = today;
    }

    const startDateStr = moment(startDate).format('YYYY-MM-DD');
    const endDateStr = moment(endDate).format('YYYY-MM-DD');

    logging.info(`Executando API de formalização para o período ${startDateStr} a ${endDateStr}...`);
    const data1 = await fetchDatapgApi(startDateStr, endDateStr);
    await upsertDataPg(data1);
    await sleep(15000); // 15 segundos

    startDate.setDate(startDate.getDate() + 15);
    endDate.setDate(startDate.getDate() + 14);

    if (endDate > today) {
        endDate = today;
    }

    const startDateStr2 = moment(startDate).format('YYYY-MM-DD');
    const endDateStr2 = moment(endDate).format('YYYY-MM-DD');

    logging.info(`Executando API de formalização para o período ${startDateStr2} a ${endDateStr2}...`);
    const data2 = await fetchDatapgApi(startDateStr2, endDateStr2);
    await upsertDataPg(data2);
}

// Função para dormir por um tempo específico
function sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

// Iniciar o script
main().catch(err => logging.error(err));
