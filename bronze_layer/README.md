# Bronze Layer
---


#### Realiza a primera etapa do ETL de dados do RAIS, processando os dados originais e conformando-os. Para essa primeira etapa, algumas considerações sobre os dados originais precisam ser consideradas devidas a fatores externos:

1. Os dados originais estão em formatos de arquivos e foram extarídos do [Ministério do Trabalho e Emprego](https://www.gov.br/trabalho-e-emprego/pt-br/assuntos/estatisticas-trabalho/microdados-rais-e-caged) e da organização [Base de Dados](https://basedosdados.org/dataset/3e7c4d58-96ba-448e-b053-d385a829ef00?table=c3a5121e-f00d-41ff-b46f-bd26be8d4af3&utm_term=base%20de%20dados%20rais&utm_campaign=Conjuntos+de+dados+-+Gratuito&utm_source=adwords&utm_medium=ppc&hsa_acc=9488864076&hsa_cam=20482085189&hsa_grp=152721262276&hsa_ad=670746326631&hsa_src=g&hsa_tgt=kwd-427920687310&hsa_kw=base%20de%20dados%20rais&hsa_mt=b&hsa_net=adwords&hsa_ver=3&gad_source=1&gad_campaignid=20482085189&gbraid=0AAAAApsIj8xku07UiShCjgiTU1J8c5py0&gclid=CjwKCAjw89jGBhB0EiwA2o1OnwNx6h_pqBRqW-lRuqGoxTk8dbhr1UT-EZL9yoJV-166Cdp_jxeL3xoCU9AQAvD_BwE) que disponibiliza dados governamentais gratuitamente.

2. Os dados de alguns anos específicos estão corrompidos quando acessados pelo site oficial do Ministério do Trabalho e Emprego, os quais foram extraídos da segunda fonte, fazendo com que os dados estejam inicialmente em dois formatos diferentes (txt e csv).

---

#### Nesta primeira etapa foi realizado os seguintes tratamentos:

1. Configura os caminhos dos diretórios dos dados brutos e de saída

2. Acessa e normaliza os arquivos de dados brutos

3. Salva os dados em formato parquet para conformidade de tipo de arquivo no diretório ```data/conformed/estabelecimentos```, agora prontos para serem utilizados na etapa silver

