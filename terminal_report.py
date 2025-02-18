import duckdb
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.layout import Layout
from rich.text import Text
from rich.prompt import Prompt
import pandas as pd
from datetime import datetime, timedelta

console = Console()

def format_number(value):
    """Format numbers for better readability"""
    if isinstance(value, (int, float)):
        if value >= 1000000:
            return f"{value/1000000:.1f}M"
        elif value >= 1000:
            return f"{value/1000:.1f}K"
        elif isinstance(value, float):
            return f"{value:.2f}"
    return str(value)

def create_summary_report(conn):
    """Generate summary metrics"""
    results = conn.execute("""
        SELECT 
            COUNT(*) as total_orders,
            COUNT(DISTINCT customer_id) as unique_customers,
            SUM(quantity * base_price) as total_revenue,
            AVG(quantity * base_price) as avg_order_value,
            COUNT(DISTINCT product_id) as unique_products
        FROM ecommerce
    """).fetchone()
    
    table = Table(title="📊 E-commerce Summary Metrics", show_header=True)
    table.add_column("Metric", style="cyan")
    table.add_column("Value", style="green")
    
    metrics = [
        ("Total Orders", results[0]),
        ("Unique Customers", results[1]),
        ("Total Revenue", f"${format_number(results[2])}"),
        ("Average Order Value", f"${format_number(results[3])}"),
        ("Unique Products", results[4])
    ]
    
    for metric, value in metrics:
        table.add_row(metric, str(value))
    
    return table

def create_category_report(conn):
    """Generate category performance report"""
    results = conn.execute("""
        SELECT 
            category_info_main,
            COUNT(*) as order_count,
            SUM(quantity * base_price) as revenue,
            AVG(review_score) as avg_rating,
            COUNT(DISTINCT customer_id) as unique_customers
        FROM ecommerce
        GROUP BY category_info_main
        ORDER BY revenue DESC
    """).fetchall()
    
    table = Table(title="📈 Category Performance", show_header=True)
    table.add_column("Category", style="cyan")
    table.add_column("Orders", style="green", justify="right")
    table.add_column("Revenue", style="green", justify="right")
    table.add_column("Avg Rating", style="yellow", justify="right")
    table.add_column("Customers", style="magenta", justify="right")
    
    for row in results:
        table.add_row(
            row[0],
            format_number(row[1]),
            f"${format_number(row[2])}",
            f"{row[3]:.1f}",
            format_number(row[4])
        )
    
    return table

def create_daily_trend_report(conn):
    """Generate daily trends report"""
    results = conn.execute("""
        SELECT 
            DATE_TRUNC('day', timestamp) as sale_date,
            COUNT(*) as orders,
            SUM(quantity * base_price) as revenue,
            COUNT(DISTINCT customer_id) as customers
        FROM ecommerce
        GROUP BY 1
        ORDER BY 1 DESC
        LIMIT 7
    """).fetchall()
    
    table = Table(title="📅 Last 7 Days Trend", show_header=True)
    table.add_column("Date", style="cyan")
    table.add_column("Orders", style="green", justify="right")
    table.add_column("Revenue", style="green", justify="right")
    table.add_column("Customers", style="magenta", justify="right")
    
    for row in results:
        table.add_row(
            row[0].strftime("%Y-%m-%d"),
            format_number(row[1]),
            f"${format_number(row[2])}",
            format_number(row[3])
        )
    
    return table

def create_top_products_report(conn):
    """Generate top products report"""
    results = conn.execute("""
        SELECT 
            p.product_id,
            p.category_info_sub as category,
            p.total_sales,
            p.avg_rating,
            p.avg_price
        FROM product_performance p
        ORDER BY p.total_sales DESC
        LIMIT 5
    """).fetchall()
    
    table = Table(title="🏆 Top 5 Products", show_header=True)
    table.add_column("Product ID", style="cyan")
    table.add_column("Category", style="blue")
    table.add_column("Sales", style="green", justify="right")
    table.add_column("Rating", style="yellow", justify="right")
    table.add_column("Avg Price", style="magenta", justify="right")
    
    for row in results:
        table.add_row(
            row[0][:8] + "...",
            row[1],
            format_number(row[2]),
            f"{row[3]:.1f}" if row[3] else "N/A",
            f"${format_number(row[4])}"
        )
    
    return table

def main():
    """Main report interface"""
    conn = duckdb.connect('ecommerce.duckdb')
    
    while True:
        console.clear()
        console.print("[bold cyan]E-commerce Analytics Dashboard[/bold cyan]", justify="center")
        console.print("=" * 80, justify="center")
        
        # Menu options
        console.print("\n[bold]Available Reports:[/bold]")
        console.print("1. Summary Metrics")
        console.print("2. Category Performance")
        console.print("3. Daily Trends")
        console.print("4. Top Products")
        console.print("5. All Reports")
        console.print("6. Exit")
        
        choice = Prompt.ask("\nSelect report", choices=["1", "2", "3", "4", "5", "6"])
        
        console.clear()
        
        if choice == "1":
            console.print(create_summary_report(conn))
        elif choice == "2":
            console.print(create_category_report(conn))
        elif choice == "3":
            console.print(create_daily_trend_report(conn))
        elif choice == "4":
            console.print(create_top_products_report(conn))
        elif choice == "5":
            layout = Layout()
            layout.split_column(
                Layout(create_summary_report(conn)),
                Layout(create_category_report(conn)),
                Layout(create_daily_trend_report(conn)),
                Layout(create_top_products_report(conn))
            )
            console.print(layout)
        elif choice == "6":
            console.print("[bold red]Exiting...[/bold red]")
            break
        
        if choice != "6":
            input("\nPress Enter to continue...")
    
    conn.close()

if __name__ == "__main__":
    main()