import os
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from sqlalchemy.types import DateTime, String, Integer, \
    Numeric, UUID, BigInteger


def count_csv_lines(file_path):
    with open(file_path) as f:
        return sum(1 for _ in f) - 1  # -1 pour l'en-tête


def main():
    load_dotenv()
    DATABASE_URL = os.getenv("DATABASE_URL")
    engine = create_engine(DATABASE_URL)

    csv_path = "/data_2022_oct.csv"
    chunksize = 100000

    # Compter le nombre total de lignes dans le CSV
    total_lines = count_csv_lines(csv_path)

    # Lire les chunks un par un
    chunks = pd.read_csv(csv_path, chunksize=chunksize)

    dtype_dict = {
        "event_time": DateTime(timezone=True),  # Type 1: Pour les timestamps
        "event_type": String,  # Type 2: Pour les types d'événements
        "product_id": Integer,  # Type 3: Pour les IDs de produits
        "price": Numeric(10, 2),  # Type 4: Pour les prix
        "user_id": BigInteger,  # Type 5: Pour les grands nombres d'IDs
        "user_session": UUID,  # Type 6: Pour les sessions UUID
    }

    # Utiliser le premier chunk pour créer la table
    first_chunk = next(chunks)
    table_name = os.path.splitext(os.path.basename(csv_path))[0]

    # Créer la table avec le premier chunk
    first_chunk.to_sql(
        table_name, engine, if_exists="replace", index=False, dtype=dtype_dict
    )
    print(f"\nTable '{table_name}' créée avec succès")

    # Traiter le reste des chunks
    for chunk in chunks:
        chunk.to_sql(
            table_name, engine, if_exists="append",
            index=False, dtype=dtype_dict
        )

    # Vérifier le nombre de lignes dans la table PostgreSQL
    with engine.connect() as connection:
        result = connection.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
        db_count = result.scalar()

    print("\nRésumé final :")
    print(f"Lignes dans le CSV : {total_lines:,}")
    print(f"Lignes dans la table PostgreSQL : {db_count:,}")


if __name__ == "__main__":
    main()
