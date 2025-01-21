import pandas as pd
class DataAnalyzer:
    def __init__(self, data):
        self.data = data
    
    def summarize_data(self, columns_name):
        """Create a summary of the data."""
        summary = {
            'Column Name': [],
            'Data Type': [],
            'Unique Values': [],
            'Repeated Values': [],
            'Missing Values': [],
        }

        for column in columns_name:
            summary['Column Name'].append(column)
            summary['Data Type'].append(self.data[column].dtype)
            summary['Unique Values'].append(self.data[column].nunique())
            summary['Repeated Values'].append(self.data[column].size - self.data[column].nunique())
            summary['Missing Values'].append(self.data[column].isnull().sum())

        return pd.DataFrame(summary)

    def calculate_statistics(self, selected_columns):
        """Calculates statistics for the selected columns."""
        results = []

        for column_name in selected_columns:
            column = self.data[column_name]
            mode_value = round(column.mode()[0], 2)
            variance_value = round(column.var(), 2)
            skewness_value = round(column.skew(), 2)
            kurtosis_value = round(column.kurt(), 2)

            results.append({
                'Mode': mode_value,
                'Variance': variance_value,
                'Skewness': skewness_value,
                'Kurtosis': kurtosis_value,
            })

        return pd.DataFrame(results, index=selected_columns).reset_index().rename(columns={'index': 'Column Name'})

    def analyze_and_combine(self, selected_columns):
        """Combines the summary, description, and statistics of the DataFrame."""
        # Create a summary of the data
        summary_df = self.summarize_data(selected_columns)

        # Get the description of the DataFrame
        describe_df = self.data[selected_columns].describe().T.reset_index()
        describe_df.rename(columns={'index': 'Column Name'}, inplace=True)

        # Calculate statistics for the selected columns
        statistics_df = self.calculate_statistics(selected_columns)

        # Combine all DataFrames into one, ensuring only one row per column
        combined_df = pd.merge(summary_df, describe_df, on='Column Name', how='outer')
        combined_df = pd.merge(combined_df, statistics_df, on='Column Name', how='outer')

        return combined_df
    
    def analize_categorical_data(self, selected_columns):
        """Transforms selected columns to categorical and combines summary, description, and statistics."""

        # Create a summary of the data
        summary_df = self.summarize_data(selected_columns)
        
        # Transform all selected columns to type 'object'
        self.data[selected_columns] = self.data[selected_columns].astype('object')
        
        # Get the description for the categorical columns
        describe_df = self.data[selected_columns].describe().T.reset_index()
        describe_df.rename(columns={'index': 'Column Name'}, inplace=True)

        # Combine summary and describe DataFrames
        combined_df = pd.merge(summary_df, describe_df, on='Column Name', how='outer')

        return combined_df
    

