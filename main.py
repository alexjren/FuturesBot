from data_handler import refresh_data
from notifier import send_discord_message
from visualize_data import plot_account_value

if __name__ == "__main__":

    """     refresh_data('30m.json', multiplier = 30, timespan='minute')
    plot_account_value('30m.json')   
    send_discord_message('30m.json') """

    refresh_data()
    plot_account_value()   
    send_discord_message()
