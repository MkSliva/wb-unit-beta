import React, { useState, useEffect } from "react";
import { DataGrid } from "@mui/x-data-grid";
import { Button } from "@mui/material";

const fields = [
  "purchase_price",
  "delivery_to_warehouse",
  "wb_commission_rub",
  "wb_logistics",
  "tax_rub",
  "packaging",
  "fuel",
  "gift",
  "real_defect_percent",
  "defect_percent",
];

const UnitEconomics = ({ goBack }) => {
  const [rows, setRows] = useState([]);
  const [filterMissing, setFilterMissing] = useState(false);

  useEffect(() => {
    fetch("http://localhost:8000/api/latest_costs_all")
      .then((r) => r.json())
      .then((d) =>
        setRows(
          d.map((row) => ({ id: row.vendor_code, ...row, start_date: "", end_date: "" }))
        )
      )
      .catch((err) => console.error("Failed to fetch costs", err));
  }, []);

  const handleCellEditCommit = (params) => {
    const { id, field, value } = params;
    setRows((prev) => prev.map((row) => (row.id === id ? { ...row, [field]: value } : row)));
  };

  const saveRow = async (row) => {
    const payload = {
      vendorcode: row.vendor_code,
      start_date: row.start_date || new Date().toISOString().split("T")[0],
    };
    if (row.end_date) payload.end_date = row.end_date;
    fields.forEach((f) => {
      if (row[f] !== undefined && row[f] !== null && row[f] !== "") {
        payload[f] = parseFloat(row[f]);
      }
    });
    try {
      const resp = await fetch("http://localhost:8000/api/update_costs", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      });
      if (resp.ok) {
        alert("Закупочная цена обновлена");
      }
    } catch (e) {
      console.error("Failed to save", e);
    }
  };

  const columns = [
    { field: "vendor_code", headerName: "Артикул", width: 120 },
    { field: "profit_per_item", headerName: "profit_per_item", type: "number", width: 150 },
    ...fields.map((f) => ({ field: f, headerName: f, type: "number", editable: true, width: 150 })),
    { field: "start_date", headerName: "Дата начала", editable: true, width: 130 },
    { field: "end_date", headerName: "Дата конца", editable: true, width: 130 },
    {
      field: "actions",
      headerName: "",
      sortable: false,
      width: 130,
      renderCell: (params) => (
        <Button variant="outlined" size="small" onClick={() => saveRow(params.row)}>
          Сохранить
        </Button>
      ),
    },
  ];

  const displayedRows = filterMissing
    ? rows.filter((item) => fields.some((f) => !item[f] || item[f] === 0))
    : rows;

  return (
    <div className="container mx-auto p-4">
      <div className="flex justify-between items-center mb-4">
        <h1 className="text-2xl font-bold">Данные Юнит Экономики</h1>
        <button onClick={goBack} className="px-4 py-2 bg-gray-300 rounded hover:bg-gray-400">
          Назад
        </button>
      </div>
      <label className="block mb-2">
        <input
          type="checkbox"
          checked={filterMissing}
          onChange={(e) => setFilterMissing(e.target.checked)}
          className="mr-2"
        />
        Показать только незаполненные
      </label>
      <div style={{ height: 500, width: "100%" }}>
        <DataGrid
          rows={displayedRows}
          columns={columns}
          onCellEditCommit={handleCellEditCommit}
          density="compact"
        />
      </div>
    </div>
  );
};

export default UnitEconomics;
