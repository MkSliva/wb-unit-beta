import React, { useState, useEffect } from "react";
// Render a plain HTML table so the page works even without extra libraries

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

  const handleChange = (id, field, value) => {
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

  const [sortField, setSortField] = useState(null);
  const [sortAsc, setSortAsc] = useState(true);

  const displayedRows = filterMissing
    ? rows.filter((item) => fields.some((f) => !item[f] || item[f] === 0))
    : rows;

  const sortedRows = React.useMemo(() => {
    if (!sortField) return displayedRows;
    return [...displayedRows].sort((a, b) => {
      if (a[sortField] === b[sortField]) return 0;
      return a[sortField] > b[sortField] ? (sortAsc ? 1 : -1) : sortAsc ? -1 : 1;
    });
  }, [displayedRows, sortField, sortAsc]);

  const handleSort = (field) => {
    if (sortField === field) {
      setSortAsc(!sortAsc);
    } else {
      setSortField(field);
      setSortAsc(true);
    }
  };

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
      <div className="overflow-auto" style={{ maxHeight: 500 }}>
        <table className="min-w-full text-sm border border-gray-300">
          <thead className="sticky top-0 bg-gray-100">
            <tr>
              <th className="p-1 border" onClick={() => handleSort('vendor_code')}>Артикул</th>
              <th className="p-1 border" onClick={() => handleSort('profit_per_item')}>profit_per_item</th>
              {fields.map((f) => (
                <th key={f} className="p-1 border" onClick={() => handleSort(f)}>{f}</th>
              ))}
              <th className="p-1 border" onClick={() => handleSort('start_date')}>Дата начала</th>
              <th className="p-1 border" onClick={() => handleSort('end_date')}>Дата конца</th>
              <th className="p-1 border"> </th>
            </tr>
          </thead>
          <tbody>
            {sortedRows.map((row) => (
              <tr key={row.id} className="odd:bg-gray-50">
                <td className="p-1 border whitespace-nowrap max-w-[8rem] truncate">{row.vendor_code}</td>
                <td className="p-1 border">{row.profit_per_item}</td>
                {fields.map((f) => (
                  <td key={f} className="p-1 border">
                    <input
                      type="number"
                      value={row[f] ?? ''}
                      onChange={(e) => handleChange(row.id, f, e.target.value)}
                      className="w-24 border rounded p-0.5"
                    />
                  </td>
                ))}
                <td className="p-1 border">
                  <input
                    type="date"
                    value={row.start_date}
                    onChange={(e) => handleChange(row.id, 'start_date', e.target.value)}
                    className="border rounded p-0.5"
                  />
                </td>
                <td className="p-1 border">
                  <input
                    type="date"
                    value={row.end_date}
                    onChange={(e) => handleChange(row.id, 'end_date', e.target.value)}
                    className="border rounded p-0.5"
                  />
                </td>
                <td className="p-1 border">
                  <button
                    className="px-2 py-1 bg-blue-500 text-white rounded"
                    onClick={() => saveRow(row)}
                  >
                    Сохранить
                  </button>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
};

export default UnitEconomics;
