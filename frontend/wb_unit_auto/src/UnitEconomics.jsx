import React, { useState, useEffect } from "react";

const fields = [
  "purchase_price",
  "delivery_to_warehouse",
  "wb_commission_rub",
  "wb_logistics",
  "tax_rub",
  "packaging",
  "fuel",
  "gift",
  "defect_percent",
];

const UnitEconomics = ({ goBack }) => {
  const [data, setData] = useState([]);
  const [filterMissing, setFilterMissing] = useState(false);
  const [editRows, setEditRows] = useState({});

  useEffect(() => {
    fetch("http://localhost:8000/api/latest_costs_all")
      .then((r) => r.json())
      .then((d) => setData(d))
      .catch((err) => console.error("Failed to fetch costs", err));
  }, []);

  const handleChange = (vendor, field, value) => {
    setEditRows((prev) => ({
      ...prev,
      [vendor]: { ...prev[vendor], [field]: value },
    }));
  };

  const saveRow = async (vendor) => {
    const row = editRows[vendor] || {};
    const payload = {
      vendorcode: vendor,
      start_date: row.start_date || new Date().toISOString().split("T")[0],
    };
    if (row.end_date) {
      payload.end_date = row.end_date;
    }
    fields.forEach((f) => {
      if (row[f] !== undefined) payload[f] = parseFloat(row[f]);
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

  const displayed = data.filter((item) => {
    if (!filterMissing) return true;
    return fields.some((f) => !item[f] || item[f] === 0);
  });

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
      <div className="overflow-x-auto">
        <table className="min-w-full divide-y divide-gray-200 border">
          <thead className="bg-gray-50">
            <tr>
              <th className="px-4 py-2 text-left">Артикул</th>
              {fields.map((f) => (
                <th key={f} className="px-4 py-2 text-left">{f}</th>
              ))}
              <th className="px-4 py-2" />
            </tr>
          </thead>
          <tbody className="bg-white divide-y divide-gray-200">
            {displayed.map((item) => (
              <tr key={item.vendor_code}>
                <td className="px-4 py-2 text-sm font-medium">{item.vendor_code}</td>
                {fields.map((f) => (
                  <td key={f} className="px-4 py-2">
                    <input
                      type="number"
                      value={
                        editRows[item.vendor_code]?.[f] ?? item[f] ?? ""
                      }
                      onChange={(e) => handleChange(item.vendor_code, f, e.target.value)}
                      className="border p-1 rounded w-24"
                    />
                  </td>
                ))}
                <td className="px-4 py-2">
                  <input
                    type="date"
                    value={editRows[item.vendor_code]?.start_date || ""}
                    onChange={(e) => handleChange(item.vendor_code, "start_date", e.target.value)}
                    className="border p-1 rounded"
                  />
                  <input
                    type="date"
                    value={editRows[item.vendor_code]?.end_date || ""}
                    onChange={(e) => handleChange(item.vendor_code, "end_date", e.target.value)}
                    className="border p-1 rounded ml-2"
                  />
                  <button
                    onClick={() => saveRow(item.vendor_code)}
                    className="ml-2 px-2 py-1 bg-blue-500 text-white rounded"
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

