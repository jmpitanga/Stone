import psycopg2
import pandas as pd

con = psycopg2.connect(database="postgres", user="read-only-user", password="banking123", host="db-stone-challenge.cjepwwjnksng.us-east-1.rds.amazonaws.com", port="5432")

transact = pd.read_sql('select c.card_family, c.card_number, count(t.id), sum(t.value) from transactions t, cards c where t.card_number = c.card_number group by c.card_family, c.card_number order by c.card_family', con)
diamond = pd.read_sql('select a.id, a.segment, q2.tc from customers a, (select customer_id, tc from (select a.customer_id, count(b.id) as tc from cards a, transactions b where a.card_number = b.card_number group by 1) as q1 where tc >= 40) as q2 where a.id = q2.customer_id and a.segment = 'Diamond'', con)

fraud = pd.read_sql('select * from (select x.id as c_id, x.age as c_age, x.segment as c_segment, x.vintage_group as c_vintage_group, c_card_number, c_card_family, c_credit_limit, c_customer_id, fraud_flag, f_tr_id, t_transaction_id, t_card_number, t_transaction_date, t_value, t_segment from customers x, (select c.card_number as c_card_number, c.card_family as c_card_family, c.credit_limit as c_credit_limit, c.customer_id as c_customer_id, fraud_flag, f_tr_id, t_transaction_id, t_card_number, t_transaction_date, t_value, t_segment from cards c, (select f.fraud_flag as fraud_flag, f.transaction_id as f_tr_id, t.id as t_transaction_id, t.card_number as t_card_number, t.transaction_date as t_transaction_date, t.value as t_value, t.segment as t_segment from frauds f, transactions t where f.transaction_id = t.id) as q1 where c.card_number = q1.t_card_number) as q2 where x.id = q2.c_customer_id) as q3', con)

transact_csv = transact.to_csv (r'..\transact.csv', index = None, header=True)
diamond_csv = diamond.to_csv (r'..\diamond.csv', index = None, header=True)

con.close()

transact_csv
diamond_csv

fraud

#Conforme os dados no dataset 'fraud', é possível perceber que a maioria das fraudes são cometidas por clientes de segmento 'Diamond'/'VG1', e,
#ainda nessas características, com cartões tipo 'Gold'. O que leva à pergunta de por que esses clientes são Diamond(?).
#Outro ponto é que o inverso acontece em relação aos clientes de segmento 'Gold', se utilizam de cartões 'Premium' em sua maioria. O que nos leva
#a prestar atenção nos contrastes entre segmento de cliente e familia de cartão.
#O limite do cartão aparentemente é irrelevante pois as fraudes não ultrapassam o valor de '50000', mas existem casos em que o valor da transação
#é maior que o limite do cartão, acontecem na familia de cartão 'Gold' em aproximadamente 46% dos casos de fraude nesta familia.