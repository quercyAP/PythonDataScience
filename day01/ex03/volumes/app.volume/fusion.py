import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv


def fusion():
    """
    Fusionne les tables 'customers' et 'items' en conservant toutes les informations.
    La fusion se fait sur la colonne 'product_id' qui est commune aux deux tables.
    """
    # Charger les variables d'environnement
    load_dotenv()
    DATABASE_URL = os.getenv("DATABASE_URL")

    # Créer la connexion à la base de données
    engine = create_engine(DATABASE_URL)

    try:
        with engine.connect() as connection:
            # Vérifier l'existence des tables
            tables_check_query = """
            SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'customers') as customers_exists,
                   EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'items') as items_exists
            """
            result = connection.execute(text(tables_check_query))
            tables_status = result.fetchone()
            
            if not tables_status[0]:
                raise Exception("La table 'customers' n'existe pas")
            if not tables_status[1]:
                raise Exception("La table 'items' n'existe pas")

            # Compter le nombre initial de lignes dans chaque table
            count_customers = connection.execute(text("SELECT COUNT(*) FROM customers")).scalar()
            count_items = connection.execute(text("SELECT COUNT(*) FROM items")).scalar()
            print(f"\nNombre de lignes dans customers: {count_customers:,}")
            print(f"Nombre de lignes dans items: {count_items:,}")

            # Créer la table 'customers_enriched' qui contiendra la fusion
            # Utiliser une jointure LEFT pour garder tous les enregistrements de 'customers'
            # même s'il n'y a pas de correspondance dans 'items'
            fusion_query = """
            CREATE TABLE customers_enriched AS
            SELECT 
                c.event_time,
                c.event_type,
                c.product_id,
                c.price,
                c.user_id,
                c.user_session,
                i.category_id,
                i.category_code,
                i.brand
            FROM 
                customers c
            LEFT JOIN 
                items i ON c.product_id = i.product_id
            """
            
            # Supprimer la table fusion si elle existe déjà
            connection.execute(text("DROP TABLE IF EXISTS customers_enriched"))
            
            # Exécuter la fusion
            print("Fusion des tables en cours...")
            connection.execute(text(fusion_query))
            connection.commit()
            
            # Vérifier le résultat
            count_fusion = connection.execute(text("SELECT COUNT(*) FROM customers_enriched")).scalar()
            print(f"Nombre de lignes dans la table fusionnée: {count_fusion:,}")
            
            # Vérifier que nous n'avons pas perdu de données de customers
            if count_fusion != count_customers:
                print("⚠️ Attention: Le nombre de lignes dans la table fusionnée ne correspond pas à celui de customers.")
            else:
                print("✅ Toutes les données de customers ont été conservées.")
            
            # Vérifier les correspondances avec items
            match_count = connection.execute(text("""
            SELECT COUNT(*) FROM customers_enriched WHERE category_id IS NOT NULL
            """)).scalar()
            match_percent = (match_count / count_fusion) * 100 if count_fusion > 0 else 0
            print(f"Pourcentage d'enregistrements avec correspondance dans items: {match_percent:.2f}%")
            
            # Renommer la table customers_enriched en customers
            print("Remplacement de la table customers par la table enrichie...")
            connection.execute(text("DROP TABLE IF EXISTS customers_old"))
            connection.execute(text("ALTER TABLE customers RENAME TO customers_old"))
            connection.execute(text("ALTER TABLE customers_enriched RENAME TO customers"))
            connection.commit()
            
            print("Fusion terminée avec succès.")
            
    except Exception as e:
        print(f"Erreur lors de la fusion: {str(e)}")


def test_fusion():
    """
    Vérifie que la fusion a correctement fonctionné en examinant la structure
    et le contenu de la table fusionnée.
    """
    # Charger les variables d'environnement
    load_dotenv()
    DATABASE_URL = os.getenv("DATABASE_URL")

    # Créer la connexion à la base de données
    engine = create_engine(DATABASE_URL)

    try:
        with engine.connect() as connection:
            print("\n--- Test de la fusion ---")
            
            # Vérifier la structure de la table
            columns_query = """
            SELECT column_name FROM information_schema.columns 
            WHERE table_name = 'customers' 
            ORDER BY ordinal_position
            """
            result = connection.execute(text(columns_query))
            columns = [row[0] for row in result.fetchall()]
            
            print("Structure de la table fusionnée:")
            for col in columns:
                print(f"  - {col}")
            
            # Vérifier si les colonnes d'items sont présentes
            items_columns = ['category_id', 'category_code', 'brand']
            missing_columns = [col for col in items_columns if col not in columns]
            
            if missing_columns:
                print(f"❌ Test échoué: Colonnes manquantes de la table items: {', '.join(missing_columns)}")
            else:
                print("✅ Test réussi: Toutes les colonnes d'items ont été ajoutées")
            
            # Afficher quelques exemples d'enregistrements fusionnés
            sample_query = """
            SELECT * FROM customers 
            WHERE category_id IS NOT NULL 
            LIMIT 5
            """
            result = connection.execute(text(sample_query))
            samples = result.fetchall()
            
            if samples:
                print("\nExemples d'enregistrements fusionnés:")
                for sample in samples:
                    print(f"  - Event: {sample.event_type}, Product: {sample.product_id}, Category: {sample.category_code}, Brand: {sample.brand}")
            else:
                print("❌ Test échoué: Aucun enregistrement avec des données d'items trouvé")
                
    except Exception as e:
        print(f"Erreur lors du test de la fusion: {str(e)}")


if __name__ == "__main__":
    fusion()
    test_fusion()
