from kimball.observability.bus_matrix import generate_bus_matrix


def test_generate_bus_matrix(tmp_path):
    # Create dummy configs - Kimball: facts use merge_keys, no surrogate_key
    (tmp_path / "fact_sales.yml").write_text(
        """
    table_name: fact_sales
    table_type: fact
    merge_keys: [transaction_id]
    sources:
      - name: dim_customer
      - name: dim_product
      - name: raw_transactions
    """,
        encoding="utf-8",
    )

    (tmp_path / "fact_inventory.yml").write_text(
        """
    table_name: fact_inventory
    table_type: fact
    merge_keys: [inventory_id]
    sources:
      - name: dim_product
      - name: dim_warehouse
    """,
        encoding="utf-8",
    )

    md = generate_bus_matrix(str(tmp_path))

    print(md)

    # Verify Header
    assert (
        "| Business Process (Fact) | dim_customer | dim_product | dim_warehouse |" in md
    )

    # Verify Rows
    # Sales has Customer and Product
    assert "| fact_sales | X | X |   |" in md
    # Inventory has Product and Warehouse
    assert "| fact_inventory |   | X | X |" in md
