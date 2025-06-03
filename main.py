# generate the 1m record CSV file
#
import polars as pl
import numpy as np
from datetime import datetime, timedelta

def generate(nrows: int, filename: str):
    names = np.asarray(
        [
            "Laptop",
            "Smartphone",
            "Desk",
            "Chair",
            "Monitor",
            "Printer",
            "Paper",
            "Pen",
            "Notebook",
            "Coffee Maker",
            "Cabinet",
            "Plastic Cups",
        ]
    )

    categories = np.asarray(
        [
            "Electronics",
            "Electronics",
            "Office",
            "Office",
            "Electronics",
            "Electronics",
            "Stationery",
            "Stationery",
            "Stationery",
            "Electronics",
            "Office",
            "Sundry",
        ]
    )

    product_id = np.random.randint(len(names), size=nrows)
    quantity = np.random.randint(1, 11, size=nrows)
    price = np.random.randint(199, 10000, size=nrows) / 100

    # Generate random dates between 2010-01-01 and 2023-12-31
    start_date = datetime(2010, 1, 1)
    end_date = datetime(2023, 12, 31)
    date_range = (end_date - start_date).days

    # Create random dates as np.array and convert to string format
    order_dates = np.array([(start_date + timedelta(days=np.random.randint(0, date_range))).strftime('%Y-%m-%d') for _ in range(nrows)])

    # Define columns
    columns = {
        "order_id": np.arange(nrows),
        "order_date": order_dates,
        "customer_id": np.random.randint(100, 1000, size=nrows),
        "customer_name": [f"Customer_{i}" for i in np.random.randint(2**15, size=nrows)],
        "product_id": product_id + 200,
        "product_names": names[product_id],
        "categories": categories[product_id],
        "quantity": quantity,
        "price": price,
        "total": price * quantity,
    }

    # Create Polars DataFrame and write to CSV with explicit delimiter
    df = pl.DataFrame(columns)
    df.write_csv(filename, separator=',',include_header=True)  # Ensure comma is used as the delimiter

# Generate 100,000 rows of data with random order_date and save to CSV
# generate(100_000, r"C:\Users\Lenovo\Desktop\test-project-task\Data_Dashboard\sales_data.csv")



import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import datetime
import psycopg2
from psycopg2 import sql
from psycopg2 import pool

# Initialize connection pool
try:
    # Create a connection pool for efficient database connections
    connection_pool = psycopg2.pool.ThreadedConnectionPool(
        minconn=5,  # Minimum number of connections in the pool
        maxconn=20, # Maximum number of connections in the pool
        dbname="postgres",
        user="postgres",
        password="1234",
        host="localhost",
        port="5432"
    )
    st.sidebar.success("Database connection pool initialized successfully")
except psycopg2.Error as e:
    st.error(f"Error creating connection pool: {e}")
    st.sidebar.error("Failed to initialize database connection")

def get_connection():
    """
    Get a connection from the pool.

    Returns:
        Connection object or None if an error occurs
    """
    try:
        conn = connection_pool.getconn()
        st.sidebar.info("Database connection acquired from pool")
        return conn
    except psycopg2.Error as e:
        st.error(f"Error getting connection from pool: {e}")
        st.sidebar.error("Failed to get database connection")
        return None

def release_connection(conn):
    """
    Release a connection back to the pool.

    Args:
        conn: The connection to release
    """
    try:
        connection_pool.putconn(conn)
        st.sidebar.info("Database connection released back to pool")
    except psycopg2.Error as e:
        st.error(f"Error releasing connection back to pool: {e}")
        st.sidebar.error("Failed to release database connection")


def get_date_range():
    """
    Retrieve the minimum and maximum dates from the sales data.

    Returns:
        Tuple containing (min_date, max_date) or (None, None) if an error occurs
    """
    conn = get_connection()
    if conn is None:
        st.warning("Could not get date range - database connection failed")
        return None, None
    try:
        with conn.cursor() as cur:
            query = sql.SQL("SELECT MIN(order_date), MAX(order_date) FROM public.sales_data")
            cur.execute(query)
            result = cur.fetchone()
            st.sidebar.success(f"Date range retrieved: {result[0]} to {result[1]}")
            return result
    except Exception as e:
        st.error(f"Error retrieving date range: {e}")
        return None, None
    finally:
        release_connection(conn)

def get_unique_categories():
    conn = get_connection()
    if conn is None:
        return []
    try:
        with conn.cursor() as cur:
            query = sql.SQL("SELECT DISTINCT categories FROM public.sales_data ORDER BY categories")
            cur.execute(query)
            return [row[0].capitalize() for row in cur.fetchall()]
    finally:
        release_connection(conn)

def get_dashboard_stats(start_date, end_date, category):
    """
    Retrieve key dashboard statistics for the selected date range and category.

    Args:
        start_date: Start date for filtering data
        end_date: End date for filtering data
        category: Product category filter (or 'All Categories')

    Returns:
        Tuple containing (total_revenue, total_orders, avg_order_value, top_category)
        or None if an error occurs
    """
    st.sidebar.info(f"Fetching dashboard stats for period: {start_date} to {end_date}, Category: {category}")
    conn = get_connection()
    if conn is None:
        st.warning("Could not get dashboard stats - database connection failed")
        return None
    try:
        with conn.cursor() as cur:
            query = sql.SQL("""
                WITH category_totals AS (
                    SELECT 
                        categories,
                        SUM(price * quantity) as category_revenue
                    FROM public.sales_data
                    WHERE order_date BETWEEN %s AND %s
                    AND (%s = 'All Categories' OR categories = %s)
                    GROUP BY categories
                ),
                top_category AS (
                    SELECT categories
                    FROM category_totals
                    ORDER BY category_revenue DESC
                    LIMIT 1
                ),
                overall_stats AS (
                    SELECT 
                        SUM(price * quantity) as total_revenue,
                        COUNT(DISTINCT order_id) as total_orders,
                        SUM(price * quantity) / COUNT(DISTINCT order_id) as avg_order_value
                    FROM public.sales_data
                    WHERE order_date BETWEEN %s AND %s
                    AND (%s = 'All Categories' OR categories = %s)
                )
                SELECT 
                    total_revenue,
                    total_orders,
                    avg_order_value,
                    (SELECT categories FROM top_category) as top_category
                FROM overall_stats
            """)
            cur.execute(query, [start_date, end_date, category, category,
                                start_date, end_date, category, category])
            result = cur.fetchone()
            st.sidebar.success(f"Dashboard stats retrieved successfully")
            return result
    except Exception as e:
        st.error(f"Error retrieving dashboard stats: {e}")
        return None
    finally:
        release_connection(conn)



def get_plot_data(start_date, end_date, category):
    """
    Retrieve time series data for plotting revenue over time.

    Args:
        start_date: Start date for filtering data
        end_date: End date for filtering data
        category: Product category filter (or 'All Categories')

    Returns:
        DataFrame containing date and revenue columns or empty DataFrame if an error occurs
    """
    st.sidebar.info(f"Fetching plot data for period: {start_date} to {end_date}, Category: {category}")
    conn = get_connection()
    if conn is None:
        st.warning("Could not get plot data - database connection failed")
        return pd.DataFrame()
    try:
        with conn.cursor() as cur:
            query = sql.SQL("""
                SELECT DATE(order_date) as date,
                       SUM(price * quantity) as revenue
                FROM public.sales_data
                WHERE order_date BETWEEN %s AND %s
                  AND (%s = 'All Categories' OR categories = %s)
                GROUP BY DATE(order_date)
                ORDER BY date
            """)
            cur.execute(query, [start_date, end_date, category, category])
            result = pd.DataFrame(cur.fetchall(), columns=['date', 'revenue'])
            st.sidebar.success(f"Retrieved {len(result)} data points for plotting")
            return result
    except Exception as e:
        st.error(f"Error retrieving plot data: {e}")
        return pd.DataFrame()
    finally:
        release_connection(conn)

def get_revenue_by_category(start_date, end_date, category):
    conn = get_connection()
    if conn is None:
        return pd.DataFrame()
    try:
        with conn.cursor() as cur:
            query = sql.SQL("""
                SELECT categories,
                       SUM(price * quantity) as revenue
                FROM public.sales_data
                WHERE order_date BETWEEN %s AND %s
                  AND (%s = 'All Categories' OR categories = %s)
                GROUP BY categories
                ORDER BY revenue DESC
            """)
            cur.execute(query, [start_date, end_date, category, category])
            return pd.DataFrame(cur.fetchall(), columns=['categories', 'revenue'])
    finally:
        release_connection(conn)

def get_top_products(start_date, end_date, category):
    conn = get_connection()
    if conn is None:
        return pd.DataFrame()
    try:
        with conn.cursor() as cur:
            query = sql.SQL("""
                SELECT product_names,
                       SUM(price * quantity) as revenue
                FROM public.sales_data
                WHERE order_date BETWEEN %s AND %s
                  AND (%s = 'All Categories' OR categories = %s)
                GROUP BY product_names
                ORDER BY revenue DESC
                LIMIT 10
            """)
            cur.execute(query, [start_date, end_date, category, category])
            return pd.DataFrame(cur.fetchall(), columns=['product_names', 'revenue'])
    finally:
        release_connection(conn)

def get_raw_data(start_date, end_date, category):
    conn = get_connection()
    if conn is None:
        return pd.DataFrame()
    try:
        with conn.cursor() as cur:
            query = sql.SQL("""
                SELECT 
                    order_id, order_date, customer_id, customer_name, 
                    product_id, product_names, categories, quantity, price, 
                    (price * quantity) as revenue
                FROM public.sales_data
                WHERE order_date BETWEEN %s AND %s
                  AND (%s = 'All Categories' OR categories = %s)
                ORDER BY order_date, order_id
            """)
            cur.execute(query, [start_date, end_date, category, category])
            return pd.DataFrame(cur.fetchall(), columns=[desc[0] for desc in cur.description])
    finally:
        release_connection(conn)

def plot_data(data, x_col, y_col, title, xlabel, ylabel, orientation='v'):
    """
    Create a matplotlib plot from the provided data.

    Args:
        data: DataFrame containing the data to plot
        x_col: Column name for x-axis
        y_col: Column name for y-axis
        title: Plot title
        xlabel: X-axis label
        ylabel: Y-axis label
        orientation: 'v' for vertical bars, 'h' for horizontal bars

    Returns:
        Matplotlib figure object
    """
    st.sidebar.info(f"Creating {orientation} plot: {title}")
    fig, ax = plt.subplots(figsize=(10, 6))
    if not data.empty:
        if orientation == 'v':
            ax.bar(data[x_col], data[y_col])
        else:
            ax.barh(data[x_col], data[y_col])
        ax.set_title(title)
        ax.set_xlabel(xlabel)
        ax.set_ylabel(ylabel)
        plt.xticks(rotation=45)
        st.sidebar.success(f"Plot created with {len(data)} data points")
    else:
        ax.text(0.5, 0.5, "No data available", ha='center', va='center')
        st.sidebar.warning("Plot created with no data")
    return fig
# for plotting the data here doing it for reference only

# Streamlit App
st.title("Sales Performance Dashboard")
st.sidebar.title("Dashboard Status")
st.sidebar.info("Application started")

# Filters
with st.container():
    st.sidebar.header("Filter Selection")
    col1, col2, col3 = st.columns([1, 1, 2])
    min_date, max_date = get_date_range()

    # Default to min_date if it exists, otherwise use a fallback date
    default_start = min_date if min_date else datetime.date(2010, 1, 1)
    # Default to max_date if it exists, otherwise use today's date
    default_end = max_date if max_date else datetime.date.today()

    start_date = col1.date_input("Start Date", default_start)
    end_date = col2.date_input("End Date", default_end)

    st.sidebar.info(f"Date range selected: {start_date} to {end_date}")

    categories = get_unique_categories()
    category = col3.selectbox("Category", ["All Categories"] + categories)
    st.sidebar.info(f"Category selected: {category}")

# Custom CSS for metrics
st.markdown("""
    <style>
    .metric-row {
        display: flex;
        justify-content: space-between;
        margin-bottom: 20px;
    }
    .metric-container {
        flex: 1;
        padding: 10px;
        text-align: center;
        background-color: #f0f2f6;
        border-radius: 5px;
        margin: 0 5px;
    }
    .metric-label {
        font-size: 14px;
        color: #555;
        margin-bottom: 5px;
    }
    .metric-value {
        font-size: 18px;
        font-weight: bold;
        color: #0e1117;
    }
    </style>
""", unsafe_allow_html=True)

# Metrics
st.header("Key Metrics")
st.sidebar.header("Metrics Calculation")
stats = get_dashboard_stats(start_date, end_date, category)
if stats:
    total_revenue, total_orders, avg_order_value, top_category = stats
    st.sidebar.success("Metrics calculated successfully")
else:
    total_revenue, total_orders, avg_order_value, top_category = 0, 0, 0, "N/A"
    st.sidebar.warning("Failed to calculate metrics, using default values")


total_revenue = total_revenue if total_revenue is not None else 0
total_orders = total_orders if total_orders is not None else 0
avg_order_value = avg_order_value if avg_order_value is not None else 0
top_category = top_category if top_category is not None else "Unknown"

st.sidebar.info(f"Total Revenue: ${total_revenue:,.2f}")
st.sidebar.info(f"Total Orders: {total_orders:,}")
st.sidebar.info(f"Average Order Value: ${avg_order_value:,.2f}")
st.sidebar.info(f"Top Category: {top_category}")

# Visualization Tabs
st.header("Visualizations")
st.sidebar.header("Visualization Generation")
tabs = st.tabs(["Revenue Over Time", "Revenue by Category", "Top Products"])

# Revenue Over Time Tab
with tabs[0]:
    st.subheader("Revenue Over Time")
    st.sidebar.info("Generating Revenue Over Time visualization")
    revenue_data = get_plot_data(start_date, end_date, category)
    st.pyplot(plot_data(revenue_data, 'date', 'revenue', "Revenue Over Time", "Date", "Revenue"))
    st.sidebar.success("Revenue Over Time visualization complete")

# Revenue by Category Tab
with tabs[1]:
    st.subheader("Revenue by Category")
    st.sidebar.info("Generating Revenue by Category visualization")
    category_data = get_revenue_by_category(start_date, end_date, category)
    st.pyplot(plot_data(category_data, 'categories', 'revenue', "Revenue by Category", "Category", "Revenue"))
    st.sidebar.success("Revenue by Category visualization complete")

# Top Products Tab
with tabs[2]:
    st.subheader("Top Products")
    st.sidebar.info("Generating Top Products visualization")
    top_products_data = get_top_products(start_date, end_date, category)
    st.pyplot(plot_data(top_products_data, 'product_names', 'revenue', "Top Products", "Revenue", "Product Name", orientation='h'))
    st.sidebar.success("Top Products visualization complete")


st.header("Raw Data")
st.sidebar.header("Raw Data Retrieval")
st.sidebar.info(f"Fetching raw data for period: {start_date} to {end_date}, Category: {category}")

raw_data = get_raw_data(
    start_date=start_date,
    end_date=end_date,
    category=category
)

# Remove the index by resetting it and dropping the old index
raw_data = raw_data.reset_index(drop=True)
st.sidebar.success(f"Retrieved {len(raw_data)} rows of raw data")

st.dataframe(raw_data,hide_index=True)

# Add spacing
st.write("")
st.sidebar.info("Dashboard rendering complete")