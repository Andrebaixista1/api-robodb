import { spawn } from 'child_process';

// Função para dormir por um tempo específico
function sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

// Função para rodar um script Node.js e aguardar sua conclusão
function runScript(script) {
    return new Promise((resolve, reject) => {
        const process = spawn('node', [script], { stdio: 'inherit' });

        process.on('close', (code) => {
            if (code === 0) {
                console.log(`✅ ${script} finalizado com sucesso!`);
                resolve();
            } else {
                console.error(`❌ Erro ao executar ${script} (Código: ${code})`);
                reject(new Error(`${script} falhou com código ${code}`));
            }
        });
    });
}

// Função principal para rodar os scripts na sequência
async function main() {
    while (true) {
        try {
            console.log("🚀 Iniciando execução dos scripts...\n");

            await runScript('cadastrados_supa.js'); // Depois, executa outro_script.js
            console.log("\n🔹 Cadastrados Supabase finalizado...\n");
            await sleep(15000); // 15 segundos
            
            await runScript('forms_supa.js'); // Depois, executa outro_script.js
            console.log("\n🔹 Formalizacao Supabase finalizado...\n");
            await sleep(15000); // 15 segundos

            await runScript('pagos_supa.js'); // Depois, executa outro_script.js
            console.log("\n🔹 Pagos Supabase finalizado...\n");
            await sleep(15000); // 15 segundos

            await runScript('pagos_local.js');  // Roda primeiro o pagos_local.js
            console.log("\n🔹 Pagos finalizado...\n");
            await sleep(15000); // 15 segundos

            await runScript('form_local.js'); // Depois, executa outro_script.js
            console.log("\n🔹 Formalização finalizado...\n");
            await sleep(15000); // 15 segundos
            
            console.log("\n✅ Todos os scripts foram executados com sucesso!");
        } catch (error) {
            console.error(`\n⚠️ Execução interrompida: ${error.message}`);
        }
    }
}

// Executa a função principal
main();
