import React, { useEffect, useState } from "react";

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

const MissingCosts = ({ startDate, endDate, goBack }) => {
  const [data, setData] = useState([]);
  const [editRows, setEditRows] = useState({});

  useEffect(() => {
    const load = async () => {
      try {
        const resp = await fetch(
          `http://localhost:8000/api/missing_costs?start_date=${startDate}&end_date=${endDate}`
        );
        if (resp.ok) {
          const list = await resp.json();
          const result = [];
          for (const item of list) {
            let costs = {};
            try {
              const c = await fetch(
                `http://localhost:8000/api/latest_costs?vendor_code=${item.vendor_code}`
              );
              if (c.ok) costs = await c.json();
            } catch {}
            result.push({ ...item, ...costs });
          }
          setData(result);
        }
      } catch (e) {
        console.error("Failed to fetch missing costs", e);
      }
    };
    load();
  }, [startDate, endDate]);

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
    fields.forEach((f) => {
      if (row[f] !== undefined) payload[f] = parseFloat(row[f]);
    });
    try {
      await fetch("http://localhost:8000/api/update_costs", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      });
    } catch (e) {
      console.error("Failed to save", e);
    }
  };

  return (
    <div className="container mx-auto p-4">
      <div className="flex justify-between items-center mb-4">
        <h1 className="text-2xl font-bold">Незаполненные расходы</h1>
        <button
          onClick={goBack}
          className="px-4 py-2 bg-gray-300 rounded hover:bg-gray-400"
        >
          Назад
        </button>
      </div>
      <div className="overflow-x-auto">
        <table className="min-w-full divide-y divide-gray-200 border">
          <thead className="bg-gray-50">
            <tr>
              <th className="px-4 py-2 text-left">Артикул</th>
              <th className="px-4 py-2 text-left">Даты заказов</th>
              {fields.map((f) => (
                <th key={f} className="px-4 py-2 text-left">
                  {f}
                </th>
              ))}
              <th className="px-4 py-2" />
            </tr>
          </thead>
          <tbody className="bg-white divide-y divide-gray-200">
            {data.map((item) => (
              <tr key={item.vendor_code}>
                <td className="px-4 py-2 text-sm font-medium">
                  {item.vendor_code}
                </td>
                <td className="px-4 py-2 text-sm">
                  {item.dates.join(", ")}
                </td>
                {fields.map((f) => (
                  <td key={f} className="px-4 py-2">
                    <input
                      type="number"
                      value={editRows[item.vendor_code]?.[f] ?? item[f] ?? ""}
                      onChange={(e) =>
                        handleChange(item.vendor_code, f, e.target.value)
                      }
                      className="border p-1 rounded w-24"
                    />
                  </td>
                ))}
                <td className="px-4 py-2">
                  <input
                    type="date"
                    value={editRows[item.vendor_code]?.start_date || ""}
                    onChange={(e) =>
                      handleChange(item.vendor_code, "start_date", e.target.value)
                    }
                    className="border p-1 rounded"
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

export default MissingCosts;

