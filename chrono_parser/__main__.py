import click
from chrono_parser.time_data_parser import TimeDataParser

@click.command()
@click.option("--filepath", "-f", required=True, help="path to sqlite-file of time-data")
def main(filepath):
    data = TimeDataParser().parse(filepath)
    print(data.head())

if __name__ == "__main__":
    main()