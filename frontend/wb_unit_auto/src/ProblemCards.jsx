import React, { useEffect, useState } from "react";

const headings = [
  { key: "imtid", label: "IMT ID" },
  { key: "vendorcodes", label: "Артикулы" },
  { key: "orderscount", label: "Заказы" },
  { key: "ad_spend", label: "Реклама" },
  { key: "total_profit", label: "Прибыль" },
  { key: "margin_percent", label: "Маржа %" },
];

const titles = {
  negative_profit: "Отрицательная прибыль",
  low_margin: "Низкая маржинальность",
  no_orders: "Отсутствие заказов",
};

const ProblemCards = ({ mode, startDate, endDate, goBack }) => {
  const [rows, setRows] = useState([]);
  const [loading, setLoading] = useState(false);

  useEffect(() => {
    const load = async () => {
      setLoading(true);
      try {
        const resp = await fetch(
          `http://localhost:8000/api/problem_cards?problem_type=${mode}&start_date=${startDate}&end_date=${endDate}`
        );
        if (resp.ok) {
          const data = await resp.json();
          setRows(data.data);
        }
      } catch (e) {
        console.error("Failed to fetch problem cards", e);
      } finally {
        setLoading(false);
      }
    };
    load();
  }, [mode, startDate, endDate]);

  return (
    <div className="p-4">
      <div className="flex justify-between items-center mb-4">
        <h1 className="text-xl font-semibold">{titles[mode]}</h1>
        <button
          onClick={goBack}
          className="px-4 py-2 bg-gray-300 rounded hover:bg-gray-400"
        >
          Назад
        </button>
      </div>
      {loading ? (
        <p>Загрузка...</p>
      ) : (
        <div className="overflow-x-auto">
          <table className="min-w-full divide-y divide-gray-200 border text-sm">
            <thead className="bg-gray-50">
              <tr>
                {headings.map((h) => (
                  <th key={h.key} className="px-4 py-2 text-left">
                    {h.label}
                  </th>
                ))}
              </tr>
            </thead>
            <tbody className="bg-white divide-y divide-gray-200">
              {rows.map((row) => (
                <tr key={row.imtid} className="hover:bg-gray-50">
                  <td className="px-4 py-2 whitespace-nowrap">{row.imtid}</td>
                  <td
                    className="px-4 py-2 whitespace-nowrap max-w-[200px] truncate"
                    title={row.vendorcodes}
                  >
                    {row.vendorcodes}
                  </td>
                  <td className="px-4 py-2">{row.orderscount}</td>
                  <td className="px-4 py-2">{row.ad_spend.toFixed(2)}</td>
                  <td
                    className={`px-4 py-2 ${row.total_profit >= 0 ? "text-green-600" : "text-red-600"}`}
                  >
                    {row.total_profit.toFixed(2)}
                  </td>
                  <td className="px-4 py-2">{row.margin_percent.toFixed(2)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
};

export default ProblemCards;
