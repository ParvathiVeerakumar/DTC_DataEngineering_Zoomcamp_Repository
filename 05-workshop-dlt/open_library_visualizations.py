import marimo

__generated_with = "0.10.16"
app = marimo.App()


@app.cell
def __():
    import marimo as mo
    import pandas as pd
    import plotly.graph_objects as go
    import dlt
    return mo, pd, go, dlt


@app.cell
def __(dlt):
    # Load data from dlt pipeline
    pipeline = dlt.pipeline(
        pipeline_name='open_library_pipeline',
        destination='duckdb',
    )
    
    # Query books per author
    with pipeline.sql_client() as client:
        authors_query = """
        SELECT value as author_name, COUNT(DISTINCT _dlt_parent_id) as book_count 
        FROM open_library_rest_api_source.harry_potter_books__author_name 
        GROUP BY value 
        ORDER BY book_count DESC 
        LIMIT 10
        """
        authors_df = client.execute_query(authors_query).fetchall()
        authors_df = pd.DataFrame(authors_df, columns=['author_name', 'book_count'])
    
    return pipeline, authors_df


@app.cell
def __(dlt, pd):
    # Query books over time
    with dlt.pipeline('open_library_pipeline', destination='duckdb').sql_client() as client:
        timeline_query = """
        SELECT first_publish_year, COUNT(*) as book_count 
        FROM open_library_rest_api_source.harry_potter_books 
        WHERE first_publish_year IS NOT NULL 
        GROUP BY first_publish_year 
        ORDER BY first_publish_year
        """
        timeline_df = client.execute_query(timeline_query).fetchall()
        timeline_df = pd.DataFrame(timeline_df, columns=['year', 'book_count'])
        timeline_df['year'] = timeline_df['year'].astype(int)
    
    return timeline_df


@app.cell
def __(authors_df, go):
    # Bar chart: Books per Author
    fig_authors = go.Figure(
        data=[
            go.Bar(
                x=authors_df['author_name'],
                y=authors_df['book_count'],
                marker=dict(color='steelblue')
            )
        ]
    )
    
    fig_authors.update_layout(
        title='Harry Potter Books by Author (Top 10)',
        xaxis_title='Author',
        yaxis_title='Number of Books',
        height=500,
        showlegend=False,
        xaxis_tickangle=-45
    )
    
    return fig_authors


@app.cell
def __(timeline_df, go):
    # Line chart: Books over Time
    fig_timeline = go.Figure(
        data=[
            go.Scatter(
                x=timeline_df['year'],
                y=timeline_df['book_count'],
                mode='lines+markers',
                line=dict(color='darkgreen', width=2),
                marker=dict(size=8)
            )
        ]
    )
    
    fig_timeline.update_layout(
        title='Harry Potter Books Published Over Time',
        xaxis_title='Year',
        yaxis_title='Number of Books',
        height=500,
        showlegend=False,
        hovermode='x unified'
    )
    
    return fig_timeline


@app.cell
def __(mo, fig_authors, fig_timeline):
    mo.vstack([
        mo.md("# Harry Potter Books Analysis"),
        mo.md("## Books by Author (Top 10)"),
        mo.plotly(fig_authors),
        mo.md("## Books Published Over Time"),
        mo.plotly(fig_timeline),
    ])


if __name__ == "__main__":
    app.run()
