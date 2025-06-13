import React, { useState, useEffect, useRef } from "react";

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
  const tableRef = useRef(null);
  const table = useRef(null);
  const [data, setData] = useState([]);
  const [filterMissing, setFilterMissing] = useState(false);

  useEffect(() => {
    fetch("http://localhost:8000/api/latest_costs_all")
      .then((r) => r.json())
      .then((d) => setData(d.map((row) => ({ ...row, start_date: "", end_date: "" }))))
      .catch((err) => console.error("Failed to fetch costs", err));
  }, []);

  useEffect(() => {
    if (!tableRef.current) return;
    const filtered = filterMissing
      ? data.filter((item) =>
          fields.some((f) => !item[f] || item[f] === 0)
        )
      : data;

    if (table.current) {
      table.current.setData(filtered);
      return;
    }

    table.current = new Tabulator(tableRef.current, {
      data: filtered,
      layout: "fitColumns",
      height: "500px",
      columns: [
        { title: "Артикул", field: "vendor_code", headerFilter: "input", width: 120 },
        { title: "profit_per_item", field: "profit_per_item", sorter: "number" },
        ...fields.map((f) => ({ title: f, field: f, editor: "input", sorter: "number" })),
        {
          title: "Дата начала",
          field: "start_date",
          editor: "input",
          editorParams: { elementAttributes: { type: "date" } },
        },
        {
          title: "Дата конца",
          field: "end_date",
          editor: "input",
          editorParams: { elementAttributes: { type: "date" } },
        },
        {
          formatter: "button",
          title: "",
          width: 120,
          formatterParams: { label: "Сохранить" },
          cellClick: (e, cell) => saveRow(cell.getRow().getData()),
        },
      ],
    });
  }, [data, filterMissing]);

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

  return (
    <div className="container mx-auto p-4">
      <div className="flex justify-between items-center mb-4">
        <h1 className="text-2xl font-bold">Данные Юнит Экономики</h1>
        <button
          onClick={goBack}
          className="px-4 py-2 bg-gray-300 rounded hover:bg-gray-400"
        >
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
      <div ref={tableRef} className="overflow-x-auto" />
    </div>
  );
};

export default UnitEconomics;
