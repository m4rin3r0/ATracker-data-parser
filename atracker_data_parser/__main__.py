import click
from atracker_data_parser.time_data_parser import TimeDataParser

@click.command()
@click.option(
    "--backup",
    "--filepath",
    "-b",
    "-f",
    "backup_path",
    required=True,
    help="path to ATracker backup (.ATracker) archive",
)
def main(backup_path):
    data = TimeDataParser().parse(backup_path)
    pass

if __name__ == "__main__":
    main()
