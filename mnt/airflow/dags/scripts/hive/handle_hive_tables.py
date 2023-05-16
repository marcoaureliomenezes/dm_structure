###############################################################################################################
#####################################   PUBLIC_DATA_TABLES  ###################################################
create_clients_table="""
            CREATE DATABASE IF NOT EXISTS clients;
            CREATE TABLE IF NOT EXISTS clients.accounts (
                hash_key string,
                nome string,
                cpf string,
                agencia bigint,
                conta bigint,
                renda_mensal double,
                saldo double,
                senha string,
                data_nascimento string,
                data_entrada string)
            ROW FORMAT DELIMITED
            FIELDS TERMINATED BY ','
            STORED AS TEXTFILE
        """
      
create_product_conta_corrente_table="""
            CREATE DATABASE IF NOT EXISTS products;
            CREATE TABLE IF NOT EXISTS products.conta_corrente (
                hash_key string,
                saldo double,
                limite bigint)
            ROW FORMAT DELIMITED
            FIELDS TERMINATED BY ','
            STORED AS TEXTFILE
        """

create_product_poupanca_table="""
            CREATE DATABASE IF NOT EXISTS products;
            CREATE TABLE IF NOT EXISTS products.poupanca (
                hash_key string,
                saldo double)
            ROW FORMAT DELIMITED
            FIELDS TERMINATED BY ','
            STORED AS TEXTFILE
        """


create_product_seguros_table="""
            CREATE DATABASE IF NOT EXISTS products;
            CREATE TABLE IF NOT EXISTS products.seguros (
                hash_key string,
                tipo_seguro string,
                valor_parcela double,
                valor_pago bigint,
                regular boolean)
            ROW FORMAT DELIMITED
            FIELDS TERMINATED BY ','
            STORED AS TEXTFILE
        """

create_product_consorcio_table="""
            CREATE DATABASE IF NOT EXISTS products;
            CREATE TABLE IF NOT EXISTS products.consorcio (
                hash_key string,
                tipo_consorcio string,
                valor_parcela double,
                parcelas_pagas bigint,
                premio_recebido boolean,
                data_inicio string,
                data_vencimento_DU string)
            ROW FORMAT DELIMITED
            FIELDS TERMINATED BY ','
            STORED AS TEXTFILE
        """

create_product_renda_fixa_table="""
            CREATE DATABASE IF NOT EXISTS products;
            CREATE TABLE IF NOT EXISTS products.renda_fixa (
                hash_key string,
                ativo string,
                total_investido bigint,
                data_inicio string)
            ROW FORMAT DELIMITED
            FIELDS TERMINATED BY ','
            STORED AS TEXTFILE
        """

create_product_renda_variavel_table="""
            CREATE DATABASE IF NOT EXISTS products;
            CREATE TABLE IF NOT EXISTS products.renda_variavel (
                hash_key string,
                categoria string,
                total_investido bigint,
                data_inicio string)
            ROW FORMAT DELIMITED
            FIELDS TERMINATED BY ','
            STORED AS TEXTFILE
        """

create_product_titulos_table="""
            CREATE DATABASE IF NOT EXISTS products;
            CREATE TABLE IF NOT EXISTS products.titulos (
                hash_key string,
                ativo string,
                total_investido bigint,
                data_inicio string)
            ROW FORMAT DELIMITED
            FIELDS TERMINATED BY ','
            STORED AS TEXTFILE
        """

create_product_derivativos_table="""
            CREATE DATABASE IF NOT EXISTS products;
            CREATE TABLE IF NOT EXISTS products.derivativos (
                hash_key string,
                categoria string,
                tipo string,
                data_inicio string)
            ROW FORMAT DELIMITED
            FIELDS TERMINATED BY ','
            STORED AS TEXTFILE
        """

create_daily_stock_markets_table="""
            CREATE DATABASE IF NOT EXISTS daily_stock_markets;
            CREATE TABLE IF NOT EXISTS daily_stock_markets.assets (
                symbol string,
                data string,
                high double,
                open double,
                close double,
                volume bigint,
                low double,
                adjclose double)
            PARTITIONED BY(asset_group string)
            ROW FORMAT DELIMITED
            FIELDS TERMINATED BY ','
            STORED AS TEXTFILE
        """


create_metadata_oracle_prices="""
            CREATE DATABASE IF NOT EXISTS oracles;
            CREATE TABLE IF NOT EXISTS oracles.metadata_pricefeeds (
                table_name string,
                network string,
                pair string,
                address string,
                phase_id bigint,
                decimals bigint,
                description string)
            ROW FORMAT DELIMITED
            FIELDS TERMINATED BY ','
            STORED AS TEXTFILE
        """

create_oracle_pricefeeds="""
            CREATE DATABASE IF NOT EXISTS oracles;
            CREATE TABLE IF NOT EXISTS oracles.pricefeeds (
                round_id string,
                answeredInRound string,
                started_at double,
                updated_at double,
                price double)
            PARTITIONED BY(pair string, network string)
            ROW FORMAT DELIMITED
            FIELDS TERMINATED BY ','
            STORED AS TEXTFILE
        """




deleting_products_database="""
            DROP TABLE IF EXISTS products.conta_corrente;
            DROP TABLE IF EXISTS products.poupanca;
            DROP TABLE IF EXISTS products.consorcio;
            DROP TABLE IF EXISTS products.seguros;
            DROP TABLE IF EXISTS products.renda_fixa;
            DROP TABLE IF EXISTS products.renda_variavel;
            DROP TABLE IF EXISTS products.titulos;
            DROP TABLE IF EXISTS products.derivativos;
            DROP DATABASE IF EXISTS products;
        """

deleting_clients_database="""
            DROP TABLE IF EXISTS daily_stock_markets.assets;
            DROP DATABASE IF EXISTS daily_stock_markets;
        """

deleting_daily_markets_database="""
            DROP TABLE IF EXISTS daily_stock_markets.assets;
            DROP DATABASE IF EXISTS daily_stock_markets;
        """

deleting_oracles_database="""
            DROP TABLE IF EXISTS oracles.metadata_pricefeeds;
            DROP TABLE IF EXISTS oracles.pricefeeds;
            DROP DATABASE IF EXISTS oracles;
        """

