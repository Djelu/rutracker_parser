import asyncio

from services import rutracker_books_parser as rutracker


def main():
    try:
        # ids = db_service.get_ids()
        parser = rutracker.Parser()
        asyncio.run(parser.run())
    except Exception as ex:
        print(ex)


if __name__ == '__main__':
    main()
