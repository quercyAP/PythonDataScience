import os
import pandas as pd
from sqlalchemy import BigInteger, create_engine, text, inspect, \
    Integer, Text, String
from dotenv import load_dotenv


def count_csv_lines(file_path):
    with open(file_path) as f:
        return sum(1 for _ in f) - 1  # -1 pour l'en-tête


def table_exists(engine, table_name):
    inspector = inspect(engine)
    return table_name in inspector.get_table_names()


def create_items_table(engine, csv_path):
    """Crée la table items avec des types de données spécifiques"""

    # Vérifier si la table existe déjà
    if table_exists(engine, "items"):
        print("\nTable 'items' existe déjà, skip...")
        return

    # Compter le nombre total de lignes
    total_lines = count_csv_lines(csv_path)
    print(f"\nTraitement de {csv_path}")

    # Définir les types de données pour certaines colonnes
    dtype_mapping = {
        "product_id": Integer,
        "category_id": BigInteger,
        "category_code": Text,
        "brand": String,
    }

    # Lire et traiter par chunks
    chunksize = 100000
    chunks = pd.read_csv(csv_path, chunksize=chunksize)

    # Premier chunk pour créer la table
    first_chunk = next(chunks)
    first_chunk.to_sql(
        "items", engine, if_exists="replace", index=False, dtype=dtype_mapping
    )

    print("Table 'items' créée")

    # Traiter les chunks restants
    for chunk in chunks:
        chunk.to_sql(
            "items", engine, if_exists="append",
            index=False, dtype=dtype_mapping
        )

    # Vérifier le nombre final de lignes
    with engine.connect() as connection:
        result = connection.execute(text("SELECT COUNT(*) FROM items"))
        db_count = result.scalar()

    print("\nRésultat pour items:")
    print(f"Lignes dans le CSV : {total_lines:,}")
    print(f"Lignes dans la table : {db_count:,}")


def main():
    load_dotenv()
    DATABASE_URL = os.getenv("DATABASE_URL")
    engine = create_engine(DATABASE_URL)

    items_csv = "/item/item.csv"

    try:
        create_items_table(engine, items_csv)

    except FileNotFoundError:
        print(f"Erreur : Le fichier {items_csv} n'existe pas")
    except Exception as e:
        print(f"Erreur lors du traitement : {str(e)}")


if __name__ == "__main__":
    main()
