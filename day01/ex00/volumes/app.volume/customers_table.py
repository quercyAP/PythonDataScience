import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv


def get_table_count(connection, table_name):
    """Get the number of rows in a table"""
    result = connection.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
    return result.scalar()


def create_customers_table():
    # Charger les variables d'environnement
    load_dotenv()
    DATABASE_URL = os.getenv("DATABASE_URL")

    # Créer la connexion à la base de données
    engine = create_engine(DATABASE_URL)

    # SQL pour créer la table customers à partir des tables existantes
    merge_query = """
    CREATE TABLE IF NOT EXISTS customers AS
    SELECT *
    FROM (
        SELECT * FROM data_2022_oct
        UNION ALL
        SELECT * FROM data_2022_nov
        UNION ALL
        SELECT * FROM data_2022_dec
        UNION ALL
        SELECT * FROM data_2023_jan
    ) AS combined_data;
    """

    source_tables = [
        "data_2022_oct",
        "data_2022_nov",
        "data_2022_dec",
        "data_2023_jan",
    ]

    try:
        with engine.connect() as connection:
            # Vérifier les nombres de lignes dans les tables sources
            print("\nNombre de lignes dans les tables sources:")
            total_source_rows = 0
            for table in source_tables:
                rows = get_table_count(connection, table)
                total_source_rows += rows
                print(f"{table}: {rows:,} lignes")

            print(f"\nTotal des lignes sources: {total_source_rows:,}")

            # Exécuter la requête de fusion
            connection.execute(text("DROP TABLE IF EXISTS customers"))
            connection.execute(text(merge_query))
            connection.commit()

            # Vérifier le nombre de lignes dans la nouvelle table
            customers_count = get_table_count(connection, "customers")
            print(f"\nTable 'customers' créée avec {customers_count:,} lignes")

    except Exception as e:
        print(f"Erreur lors de la création de la table: {str(e)}")


if __name__ == "__main__":
    create_customers_table()
