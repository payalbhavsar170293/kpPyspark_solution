import logging


def rename_column(df, old_column_name, new_column_name):
    """
    this function will rename the columns
    :param df: dataframe with schema
    :param old_column_name: old column name need to be changed
    :param new_column_name: new column name
    :return: dataframe after renaming the columns
    """
    rename_column_df = df.withColumnRenamed(old_column_name, new_column_name)
    logging.info("Column renamed successfully from old column name  %s to new column name %s", old_column_name, new_column_name)

    return rename_column_df


def filter_data(df, filter_column_name, filter_value):
    """
    this function will filter the data from given dataframe for the given column
    and filtered value
    :param df: dataframe with schema
    :param filter_column_name: column name on which filter is applied
    :param filter_value: value for which we want to filter data
    :return: filtered dataframe
    """

    filtered_df = df.filter(df[filter_column_name] == filter_value)
    logging.info("Dataframe is filter using filter column %s having value %s", filter_column_name, filter_value)
    return filtered_df


def generate_marketing_push_data(client_input_df, financial_input_df, country_name):
    """
    this function will take both  input dataframes (client_input_df & financial_input_df)
      and then transform the data in steps
      1. filter the data as per the country given as parameter in the function
      2. remove  identifiable information from the client df
      3. remove credit card number from financial df
      4. rename columns for financial_input_df
      5. join both df on id column
      6. select only required columns from joined df
    :param client_input_df: dataframe for client dataset
    :param financial_input_df: dataframe for financial dataset
    :param country_name: country name for which we want to generate output data
    :return:
    """

    # Step 1
    filter_country_client_df = filter_data(client_input_df, 'country', country_name)

    # Step 2
    remove_identifiable_column_list = ['first_name', 'last_name']
    without_identifiable_info_client_df = filter_country_client_df.drop(*remove_identifiable_column_list)

    # Step 3
    remove_cc_n_financial_df = financial_input_df.drop('cc_n')

    # Step 4
    rename_id_df = rename_column(remove_cc_n_financial_df, 'id', 'client_identifier')
    rename_btc_a_df = rename_column(rename_id_df, 'btc_a', 'bitcoin_address')
    rename_cc_t_financial_df = rename_column(rename_btc_a_df, 'cc_t', 'credit_card_type')

    # Step 5
    joined_df = without_identifiable_info_client_df.join(rename_cc_t_financial_df,
                                                         without_identifiable_info_client_df['id'] ==
                                                         rename_cc_t_financial_df['client_identifier'],
                                                         how="inner")

    # Step 6
    result_df = joined_df.select(
        'client_identifier', 'email', 'country', 'bitcoin_address', 'credit_card_type'
    )

    return result_df
