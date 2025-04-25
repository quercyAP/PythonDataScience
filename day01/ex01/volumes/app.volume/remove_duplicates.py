import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv


def remove_duplicates():
    # Charger les variables d'environnement
    load_dotenv()
    DATABASE_URL = os.getenv("DATABASE_URL")

    # Créer la connexion à la base de données
    engine = create_engine(DATABASE_URL)

    try:
        with engine.connect() as connection:
            # Compter le nombre initial de lignes
            result = connection.execute(text("SELECT COUNT(*) FROM customers"))
            initial_count = result.scalar()
            print(f"\nNombre initial de lignes: {initial_count:,}")

            # Créer une table temporaire sans doublons
            # On considère comme doublons les événements qui ont:
            # 1. Les mêmes valeurs pour TOUTES les colonnes
            # 2. Se produisent dans un intervalle de 1 seconde
            dedup_query = """
            CREATE TABLE customers_no_duplicates AS
            WITH ranked_events AS (
                SELECT *,
                    LAG(event_time) OVER (
                        PARTITION BY event_type, product_id, price, user_id, user_session
                        ORDER BY event_time
                    ) as prev_event_time
                FROM customers
            )
            SELECT 
                event_time,
                event_type,
                product_id,
                price,
                user_id,
                user_session
            FROM ranked_events
            WHERE 
                prev_event_time IS NULL 
                OR 
                EXTRACT(EPOCH FROM (event_time - prev_event_time)) > 1;
            """

            # Exécuter la déduplication
            connection.execute(text("DROP TABLE IF EXISTS customers_no_duplicates"))
            connection.execute(text(dedup_query))

            # Remplacer l'ancienne table par la nouvelle
            connection.execute(text("DROP TABLE customers"))
            connection.execute(
                text("ALTER TABLE customers_no_duplicates RENAME TO customers")
            )
            connection.commit()

            # Compter le nombre final de lignes
            result = connection.execute(text("SELECT COUNT(*) FROM customers"))
            final_count = result.scalar()

            # Afficher les statistiques
            duplicates_removed = initial_count - final_count
            print(f"Nombre final de lignes: {final_count:,}")
            print(f"Nombre de doublons supprimés: {duplicates_removed:,}")

    except Exception as e:
        print(f"Erreur lors de la suppression des doublons: {str(e)}")


def test_no_duplicates():
    """
    Vérifie qu'il n'y a plus de doublons dans la table 'customers' après déduplication,
    en vérifiant spécifiquement les instructions identiques séparées par moins d'une seconde.
    """
    # Charger les variables d'environnement
    load_dotenv()
    DATABASE_URL = os.getenv("DATABASE_URL")

    # Créer la connexion à la base de données
    engine = create_engine(DATABASE_URL)

    try:
        with engine.connect() as connection:
            print("\n--- Test de vérification des doublons ---")

            # Test 1: Vérifier s'il existe des doublons exacts (toutes colonnes identiques)
            duplicate_test_query = """
            SELECT 
                event_type, product_id, price, user_id, user_session, event_time, COUNT(*)
            FROM 
                customers
            GROUP BY 
                event_type, product_id, price, user_id, user_session, event_time
            HAVING 
                COUNT(*) > 1
            """

            result = connection.execute(text(duplicate_test_query))
            duplicates = result.fetchall()

            if duplicates:
                print(
                    f"❌ Test échoué: {len(duplicates)} groupes de doublons exacts trouvés!"
                )
                for dup in duplicates[:5]:  # Afficher les 5 premiers groupes
                    print(f"  - {dup}")
                if len(duplicates) > 5:
                    print(f"  ...et {len(duplicates) - 5} autres groupes")
            else:
                print("✅ Test réussi: Aucun doublon exact trouvé")

            # Test 2: Vérifier s'il existe des événements identiques séparés par moins d'une seconde
            time_duplicate_test_query = """
            WITH events_with_prev AS (
                SELECT 
                    event_time,
                    event_type, 
                    product_id, 
                    price, 
                    user_id, 
                    user_session,
                    LAG(event_time) OVER (
                        PARTITION BY event_type, product_id, price, user_id, user_session
                        ORDER BY event_time
                    ) as prev_event_time
                FROM 
                    customers
            )
            SELECT 
                event_time, 
                prev_event_time, 
                event_type, 
                product_id, 
                EXTRACT(EPOCH FROM (event_time - prev_event_time)) as time_diff_seconds
            FROM 
                events_with_prev
            WHERE 
                prev_event_time IS NOT NULL
                AND EXTRACT(EPOCH FROM (event_time - prev_event_time)) <= 1
            """

            result = connection.execute(text(time_duplicate_test_query))
            time_duplicates = result.fetchall()

            if time_duplicates:
                print(
                    f"❌ Test échoué: {len(time_duplicates)} paires d'événements identiques avec moins d'une seconde d'intervalle!"
                )
                for dup in time_duplicates[:5]:  # Afficher les 5 premières paires
                    print(f"  - Événement: {dup.event_type}, Produit: {dup.product_id}")
                    print(
                        f"    Temps: {dup.event_time} et {dup.prev_event_time} (différence: {dup.time_diff_seconds:.3f}s)"
                    )
                if len(time_duplicates) > 5:
                    print(f"  ...et {len(time_duplicates) - 5} autres paires")
            else:
                print(
                    "✅ Test réussi: Aucun événement identique avec moins d'une seconde d'intervalle"
                )

            # Conclusion
            if not duplicates and not time_duplicates:
                print("✅ Tous les tests réussis! La déduplication est efficace.")
            else:
                print("❌ La déduplication n'a pas correctement fonctionné.")

    except Exception as e:
        print(f"Erreur lors des tests de vérification: {str(e)}")


if __name__ == "__main__":
    remove_duplicates()
    test_no_duplicates()
