RED=\033[0;31m
GREEN=\033[0;32m
YELLOW=\033[0;33m
BLUE=\033[0;34m
MAGENTA=\033[0;35m
CYAN=\033[0;36m
RESET=\033[0m

.PHONY: all fclean clean stop start re restart create-dir

all:	create-dir update-env
		docker-compose up --build -d

fclean: stop
		docker rmi -f $$(docker images -q)
		docker image prune -a -f
		docker system prune -a -f

stop:	
		docker-compose down -v

start:	create-dir update-env
		docker-compose up -d

re:	clean fclean all

restart: stop start

create-dir:
	@echo "${BLUE}Vérification et création des dossiers nécessaires...${RESET}"
	@mkdir -p ~/goinfre/db.volume
	@mkdir -p volumes/app.volume
	@echo "${GREEN}Dossiers créés :${RESET}"
	@echo "  - ~/goinfre/db.volume"
	@echo "  - volumes/app.volume"



