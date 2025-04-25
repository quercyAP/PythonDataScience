GREEN=\033[0;32m
RED=\033[0;31m
YELLOW=\033[0;33m
BLUE=\033[0;34m
MAGENTA=\033[0;35m
CYAN=\033[0;36m
RESET=\033[0m

.PHONY: all fclean clean stop start re restart create-dir clean-db

all:	create-dir
		@bash -c 'source .env && docker-compose up --build -d'

fclean: stop
		docker rmi -f $$(docker images -q)
		docker image prune -a -f
		docker system prune -a -f

stop:
		docker-compose down -v

start:	create-dir
		@bash -c 'source .env && docker-compose up -d'

re:	clean fclean all

restart: stop start

create-dir:
	@mkdir -p ~/goinfre/db.volume volumes/app.volume
	@echo "${GREEN}Dossiers créés :${RESET}"
	@echo "  - ~/goinfre/db.volume"
	@echo "  - volumes/app.volume"

clean-db:
	@echo "Nettoyage de la base de données..."
	@if [ "$$(docker ps -q -f name=db)" ]; then \
		if ! docker exec db bash -c "rm -rf /var/lib/postgresql/data/*"; then \
			echo "Erreur lors de la suppression. Tentative de redémarrage..."; \
			docker-compose up -d db; \
			sleep 5; \
			docker exec db bash -c "rm -rf /var/lib/postgresql/data/*" || true; \
		fi \
	else \
		docker start db; \
		sleep 2; \
		if ! docker exec db bash -c "rm -rf /var/lib/postgresql/data/*"; then \
			echo "Erreur lors de la suppression. Tentative de redémarrage..."; \
			docker-compose up -d db; \
			sleep 5; \
			docker exec db bash -c "rm -rf /var/lib/postgresql/data/*" || true; \
		fi; \
		docker-compose down -v db; \
	fi
	@echo "Base de données nettoyée"
