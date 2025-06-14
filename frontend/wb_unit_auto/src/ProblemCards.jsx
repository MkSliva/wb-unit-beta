import React, { useEffect, useState } from "react";

const headings = [
  { key: "imtid", label: "IMT ID" },
  { key: "vendorcodes", label: "Артикулы" },
  { key: "orderscount", label: "Заказы" },
  { key: "ad_spend", label: "Реклама" },
  { key: "total_profit", label: "Прибыль" },
  { key: "margin_percent", label: "Маржа %" },
  { key: "subjectname", label: "Категория" },
];

const titles = {
  negative_profit: "Отрицательная прибыль",
  low_margin: "Низкая маржинальность",
  no_orders: "Отсутствие заказов",
};

const ProblemCards = ({ mode, startDate, endDate, goBack }) => {
  const [rows, setRows] = useState([]);
  const [loading, setLoading] = useState(false);
  const [sortConfig, setSortConfig] = useState({ key: null, direction: "ascending" });
  const [category, setCategory] = useState("");

  useEffect(() => {
    const load = async () => {
      setLoading(true);
      try {
        const url = new URL("http://localhost:8000/api/problem_cards");
        url.searchParams.append("problem_type", mode);
        url.searchParams.append("start_date", startDate);
        url.searchParams.append("end_date", endDate);
        if (category) url.searchParams.append("category", category);
        const resp = await fetch(url);
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
  }, [mode, startDate, endDate, category]);

  const categories = Array.from(new Set(rows.map((r) => r.subjectname).filter(Boolean)));

  const requestSort = (key) => {
    let direction = "ascending";
    if (sortConfig.key === key && sortConfig.direction === "ascending") {
      direction = "descending";
    }
    setSortConfig({ key, direction });
  };

  const getSortIndicator = (key) => {
    if (sortConfig.key === key) {
      return sortConfig.direction === "ascending" ? " ▲" : " ▼";
    }
    return "";
  };

  const sortedRows = React.useMemo(() => {
    let items = [...rows];
    if (sortConfig.key) {
      items.sort((a, b) => {
        const valA = a[sortConfig.key];
        const valB = b[sortConfig.key];
        if (valA === null || valA === undefined) return sortConfig.direction === "ascending" ? 1 : -1;
        if (valB === null || valB === undefined) return sortConfig.direction === "ascending" ? -1 : 1;
        if (valA < valB) return sortConfig.direction === "ascending" ? -1 : 1;
        if (valA > valB) return sortConfig.direction === "ascending" ? 1 : -1;
        return 0;
      });
    }
    return items;
  }, [rows, sortConfig]);

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
      <div className="mb-4">
        <label className="mr-2">Категория:</label>
        <select
          value={category}
          onChange={(e) => setCategory(e.target.value)}
          className="border p-1"
        >
          <option value="">Все</option>
          {categories.map((c) => (
            <option key={c} value={c}>
              {c}
            </option>
          ))}
        </select>
      </div>
      {loading ? (
        <p>Загрузка...</p>
      ) : (
        <div className="overflow-x-auto">
          <table className="min-w-full divide-y divide-gray-200 border text-sm">
            <thead className="bg-gray-50">
              <tr>
                {headings.map((h) => (
                  <th
                    key={h.key}
                    className="px-4 py-2 text-left cursor-pointer hover:bg-gray-100"
                    onClick={() => requestSort(h.key)}
                  >
                    {h.label} {getSortIndicator(h.key)}
                  </th>
                ))}
              </tr>
            </thead>
            <tbody className="bg-white divide-y divide-gray-200">
              {sortedRows
                .filter((r) => !category || r.subjectname === category)
                .map((row) => (
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
                  <td className="px-4 py-2">{row.subjectname}</td>
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
