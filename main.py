from data_handler import refresh_data
from notifier import send_discord_message
from visualize_data import plot_account_value

def main():
    refresh_data(process_all=True, signal_all = True)
    plot_account_value()   
    send_discord_message()

if __name__ == "__main__":
    main()