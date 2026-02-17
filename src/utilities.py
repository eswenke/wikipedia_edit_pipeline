import sys
from psql_manager import PSQLManager
from redis_manager import RedisManager


# setup and flush utility file
def main():
    if len(sys.argv) != 2:
        print("usage: python psql_setup.py [setup|flush]")
        return

    command = sys.argv[1].lower()

    if command == "setup":
        print("setting up postgresql database...")
        psql_manager = PSQLManager()
        psql_manager.setup_db()

    elif command == "flush":
        print("flushing Redis...")
        redis_manager = RedisManager()
        redis_manager.flush_db()

        print("flushing PostgreSQL...")
        psql_manager = PSQLManager()
        psql_manager.truncate_db()

    else:
        print(f"unknown command: {command}")
        print("usage: python psql_setup.py [setup|flush]")


if __name__ == "__main__":
    main()
