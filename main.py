import python_sf as sf

if __name__ == "__main__":
    with sf.SnowflakeSession() as session:
        session.execute("SELECT 1")
        result = session.fetch_pandas_all()
        print(result)
