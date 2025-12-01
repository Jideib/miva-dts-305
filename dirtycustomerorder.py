import pandas as pd
import numpy as np

# Set random seed for reproducibility
np.random.seed(42)

# Create 2500 rows
n_rows = 2500

# Generate columns
order_ids = [f'ORD{10000 + i}' for i in range(n_rows)]
customer_names = [f'Customer_{i}' for i in range(n_rows)]
order_dates = pd.date_range(start='2023-01-01', periods=n_rows, freq='h')
product_categories = np.random.choice(['Electronics', 'Clothing', 'Books', 'Home', 'Sports'], n_rows)
quantities = np.random.randint(1, 10, n_rows)
prices = np.round(np.random.uniform(10, 500, n_rows), 2)

# Introduce missing values in Total Amount (about 5%)
total_amounts = prices * quantities
mask = np.random.rand(n_rows) < 0.05
total_amounts = np.where(mask, np.nan, total_amounts)

# Countries with inconsistent formatting
countries_raw = np.random.choice(['usa', 'U.S.A.', 'United States', 'UK', 'U.K.', 'United Kingdom',
                                  'canada', 'Canada', 'ger', 'Germany', 'fr', 'France'], n_rows)

# Create DataFrame
df = pd.DataFrame({
    'Order ID': order_ids,
    'Customer Name': customer_names,
    'Order Date': order_dates,
    'Product Category': product_categories,
    'Quantity': quantities,
    'Price': prices,
    'Total Amount': total_amounts,
    'Country': countries_raw
})

# Save raw dataset first
df.to_csv('customer_orders_raw.csv', index=False)
print(f"Raw dataset saved with {len(df)} rows")

# ========== ADDING DUPLICATE ROWS ==========
duplicate_count = 200
duplicate_indices = np.random.choice(df.index, duplicate_count, replace=True)
duplicate_rows = df.loc[duplicate_indices].copy()

for idx, row in duplicate_rows.iterrows():
    duplicate_rows.at[idx, 'Order Date'] = row['Order Date'] + pd.Timedelta(seconds=1)
    duplicate_rows.at[idx, 'Quantity'] = max(1, row['Quantity'] + np.random.choice([-1, 0, 1]))

    # Recalculate Total Amount for modified quantities
    if 'Quantity' in duplicate_rows.columns and 'Price' in duplicate_rows.columns:
        duplicate_rows.at[idx, 'Total Amount'] = duplicate_rows.at[idx, 'Price'] * duplicate_rows.at[idx, 'Quantity']

# Append duplicates to original DataFrame
df_with_duplicates = pd.concat([df, duplicate_rows], ignore_index=True)

# Shuffle the DataFrame to mix duplicates with original rows
df_with_duplicates = df_with_duplicates.sample(frac=1, random_state=42).reset_index(drop=True)

# Save the dataset with duplicates
df_with_duplicates.to_csv('customer_orders_dirty.csv', index=False)

print(f"Original rows: {len(df)}")
print(f"Duplicate rows added: {len(duplicate_rows)}")
print(f"Total rows with duplicates: {len(df_with_duplicates)}")
print(f"Dataset with duplicates saved to 'customer_orders_dirty.csv'")
