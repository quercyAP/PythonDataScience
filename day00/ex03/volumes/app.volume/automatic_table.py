import os
import pandas as pd
from sqlalchemy import create_engine, text, inspect
from dotenv import load_dotenv
from sqlalchemy.types import DateTime, String, Integer, \
    Numeric, UUID, BigInteger


def count_csv_lines(file_path):
    with open(file_path) as f:
        return sum(1 for _ in f) - 1  # -1 pour l'en-tête


def table_exists(engine, table_name):
    inspector = inspect(engine)
    return table_name in inspector.get_table_names()


def process_csv_file(file_path, engine):
    """
    Traite un fichier CSV et crée une table correspondante
    dans la base de données.

    Args:
        file_path (str): Le chemin du fichier CSV.
        engine (Engine): L'engine de connexion à la base de données.

    Returns:
        None
    """
    # Obtenir le nom de la table à partir du nom du fichier
    table_name = os.path.splitext(os.path.basename(file_path))[0]

    # Vérifier si la table existe déjà
    if table_exists(engine, table_name):
        print(f"\nTable '{table_name}' existe déjà, skip...")
        return

    # Compter le nombre total de lignes
    total_lines = count_csv_lines(file_path)
    print(f"\nTraitement de {file_path}")

    # Lire et traiter par chunks
    chunksize = 100000
    chunks = pd.read_csv(file_path, chunksize=chunksize)

    dtype_dict = {
        'event_time': DateTime(timezone=True),
        'event_type': String,
        'product_id': Integer,
        'price': Numeric(10, 2),
        'user_id': BigInteger,
        'user_session': UUID
    }

    # Premier chunk pour créer la table
    first_chunk = next(chunks)
    first_chunk.to_sql(
        table_name, engine, if_exists="replace",
        index=False, dtype=dtype_dict
    )

    print(f"Table '{table_name}' créée")

    # Traiter les chunks restants
    for chunk in chunks:
        chunk.to_sql(
            table_name, engine,
            if_exists="append", index=False, dtype=dtype_dict
        )

    # Vérifier le nombre final de lignes
    with engine.connect() as connection:
        result = connection.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
        db_count = result.scalar()

    print(f"\nRésultat pour {table_name}:")
    print(f"Lignes dans le CSV : {total_lines:,}")
    print(f"Lignes dans la table : {db_count:,}")
    print("=" * 50)


def main():
    load_dotenv()
    DATABASE_URL = os.getenv("DATABASE_URL")
    engine = create_engine(DATABASE_URL)

    # Chercher tous les fichiers CSV dans le dossier customer
    customer_dir = "/customer"
    try:
        # Lister tous les fichiers du dossier
        files = os.listdir(customer_dir)
        # Filtrer pour ne garder que les .csv
        csv_files = [f for f in files if f.endswith(".csv")]

        if not csv_files:
            print("Aucun fichier CSV trouvé dans le dossier /customer/")
            return

        print(f"Fichiers CSV trouvés : {len(csv_files)}")
        for csv_file in csv_files:
            # Construire le chemin complet du fichier
            file_path = os.path.join(customer_dir, csv_file)
            process_csv_file(file_path, engine)

        print("\nTraitement terminé !")

    except FileNotFoundError:
        print(f"Erreur : Le dossier {customer_dir} n'existe pas")
    except Exception as e:
        print(f"Erreur lors du traitement : {str(e)}")


if __name__ == "__main__":
    main()
