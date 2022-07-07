import logging
import os
import click

@click.command()
@click.argument('date')
def main(date):
    """Example main function that invokes the job.

    Args:
        date (str): The date you want passed to the entrypoint of the script.
    """
    click.echo("This is the main function.")
    click.echo("Given the same inputs it should produce the exact same output.")
    click.echo(f"This is the date passed to the main function: {date}")
    
    click.echo("These are the environment variables available:\n")
    
    for k, v in os.environ.items():
        click.echo(f'    {k}={v}')

if __name__ == '__main__':
    main()
