Criar documento contendo a análise e dicionário de dados do dataset que está sendo utilizado nos trabalhos.  
  
Utilizar o horário da aula para aprofundar no framework panderas e indicar todas as verificações que podem ser úteis para validação do esquema do dataset do trabalho (conforme  tipos de dados e domínio das colunas do dataset e exemplos nas "Regras de negócio" indicadas no documento de exemplo).  
  
  
O que entregar (em um único documento):  
  
1 - Dicionário de dados do dataset   
Para o item "Schema e Dicionário de Colunas", que é o dicionário de dados, caso a origem não seja no formato parquet, incluir somente:  
Nome da Coluna, Tipo de dados (python), Nulo (S/N), Domínio (valores e/ou faixas de valores esperados), Descrição (significado da coluna) e Observações  
  
Observação:  
A seção 3 - Tabelas de Referências Auxiliares é necessária somente se precisar fazer join para ter o significado de alguma coluna que esteja codificada, conforme as do exemplo enviado.  
  
As Regras de negócio e Validações (Seção 4) servirão como base para as validações de esquema no panderas.   
  

https://pandera.readthedocs.io/en/stable/index.html