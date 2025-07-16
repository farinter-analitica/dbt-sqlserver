def define_interest_rate(account_type):
    """
    Defines interest rate based on account type

    :param account_type: Account type

    :return: Interest rate
    """
    if account_type == "saving":
        return 0.02
    elif account_type == "current":
        return 0.01
    else:
        return 0.005


def calculate_interest(account_type, balance):
    """
    Calculate interest based on account type and balance

    :param account_type: Account type
    :param balance: Balance

    :return: Interest
    """
    rate = define_interest_rate(account_type)
    return balance * rate
