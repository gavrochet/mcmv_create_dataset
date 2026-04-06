
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
ETAPA 5 — EXTRAÇÃO ESTRUTURADA
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Para cada PDF em editais_match\{cidade}\, extraia TODOS os beneficiários
vinculados ao sorteio, inclusive os que não assinaram contrato e, portanto,
não constam no CSV âncora (empreendimentos.csv). O objetivo é a lista
completa de sorteados/reserva E inscritos, não apenas os que formalizaram a aquisição.

REGRAS CRÍTICAS DE GRUPOS — NUNCA VIOLE:
1. NUNCA agregue grupos distintos num único campo. Se o edital separar
   Idosos / PCD / Mulher Chefe de Família / Lista Geral, preserve cada um.
2. Capture o nome do critério/grupo EXATAMENTE como aparece no documento.
   Não normalize, não traduza, não generalize.
3. Se houver subgrupos dentro de um grupo (ex: cotas dentro de PCD,
   categorias dentro de Lista Geral), capture o subgrupo separadamente.
4. Se a informação de grupo for ambígua, marque grupo_criterio = "AMBIGUO"
   e subgrupo = texto exato do trecho relevante do PDF.
5. Sorteados e inscritos não sorteados são grupos distintos — NUNCA os misture.
6. Se houver lista de reserva, é uma terceira categoria distinta de
   sorteado e inscrito.

CAMPOS DO OUTPUT (um linha por pessoa):
- cpf: 11 dígitos sem pontuação (se disponível no PDF)
- nis: 11 dígitos (se disponível)
- nome: nome completo (se disponível, senão NA)
- identificador_original: exatamente como aparece no PDF (cpf formatado, nis, etc)
- status: "sorteado" | "inscrito_nao_sorteado" | "reserva" | "indefinido"
- posicao_lista: número de ordem na lista (se disponível)
- grupo_criterio: texto exato do grupo/critério no edital
- subgrupo: texto exato do subgrupo (se existir, senão NA)
- empreendimento: nome_empreendimento do CSV âncora
- ano_empreendimento: ano extraído de data_moda_assinatura do CSV âncora
- cidade: cidade
- estado: UF
- arquivo_origem: nome do PDF de origem
- pagina_origem: página do PDF onde foi encontrado

Output: output\por_cidade\{cidade}.csv

Observações importantes: não é óbvio qual o grupo/critério sendo usado. 
Via de regra, esta informação estará no texto anterior à lista que queremos extrair. 
Por exemplo, pode haver um "Grupo 1", "Grupo 2", "Idosos", "Deficientes" etc no cabeçalho da lista; ou, no texto precedente à lista, pode ser que seja dito que a lista de convocados ou de inscritos/elegíveis/candidatos é referente ao grupo específico; ou que cumpre com os critérios específicos; ou pode ser que haja, na própria tabela, uma coluna específica com os critérios e grupos. Você deve fazer uma análise prévia do edital para entender como - e se - aparece em cada PDF específico. Em geral, há certa uniformidade na forma intra-cidade.

Para descobrir exatamente qual a melhor forma de prosseguir, faça o seguinte:
i) Para os PDFs com mais de 50 matches, leia (você, como IA) as páginas onde esses matches estão, bem como as 3 páginas anteriores. 
ii) Entenda como cada PDF lido está colocando as informações. 
iii) vá criando funções que extraem diferentes formatos/estilos/jeitos de mostrar as informações.
iv) Depois de várias funções diferentes, aplique todas as técnicas para os PDFs menores e apenas cheque o resultado. Caso nenhum tenha funcionado (mostrado se é Inscrito ou Sorteado, ou se está em Grupo 1, 2 etc, ou se é de pessoas idosas, com deficiência etc), leia também as páginas onde estão os matches, bem como as 2 anteriores. Se houver algum padrão novo, adicione à lista de funções. 
v) Và "guardando" essas formas de extrair as informações. Na medida em que vamos mudando de cidade, muitos padrões vão se repetir. 
