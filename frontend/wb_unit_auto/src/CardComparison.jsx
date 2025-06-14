import React, { useState, useEffect } from "react";

const filterTypes = [
  { value: "ad_manager_name", label: "Менеджер" },
  { value: "card_changes", label: "Изменение" },
  { value: "brand", label: "Бренд" },
];

const CardComparison = ({ goBack }) => {
  const [groups, setGroups] = useState([
    { type: "ad_manager_name", value: "" },
    { type: "card_changes", value: "" },
  ]);
  const [start, setStart] = useState("");
  const [end, setEnd] = useState("");
  const [results, setResults] = useState([]);
  const [managerOpts, setManagerOpts] = useState([]);
  const [changeOpts, setChangeOpts] = useState([]);
  const [brandOpts, setBrandOpts] = useState([]);

  useEffect(() => {
    fetch("http://localhost:8000/api/ad_managers")
      .then((r) => (r.ok ? r.json() : []))
      .then((d) => setManagerOpts(["0", ...d]))
      .catch(() => {});
    fetch("http://localhost:8000/api/card_change_options")
      .then((r) => (r.ok ? r.json() : []))
      .then(setChangeOpts)
      .catch(() => {});
    fetch("http://localhost:8000/api/brands")
      .then((r) => (r.ok ? r.json() : []))
      .then(setBrandOpts)
      .catch(() => {});
  }, []);

  const addGroup = () => {
    setGroups((p) => [...p, { type: "ad_manager_name", value: "" }]);
  };

  const handleChange = (idx, field, val) => {
    setGroups((p) => p.map((g, i) => (i === idx ? { ...g, [field]: val } : g)));
  };

  const fetchData = async () => {
    if (!start || !end) {
      alert("Выберите диапазон дат");
      return;
    }
    const res = [];
    for (const g of groups) {
      if (!g.value) continue;
      const params = new URLSearchParams({
        start_date: start,
        end_date: end,
        filter_field: g.type,
        filter_value: g.value,
      });
      const r = await fetch(`http://localhost:8000/api/sales_filtered_range?${params.toString()}`);
      if (r.ok) {
        const d = await r.json();
        let labelVal = g.value;
        if (g.type === "ad_manager_name" && g.value === "0") {
          labelVal = "Без менеджера";
        }
        res.push({
          label: `${g.type}: ${labelVal}`,
          total_profit: d.total_profit,
          total_ad_spend: d.total_ad_spend,
          margin_percent: d.margin_percent,
          add_to_cart_conv: d.add_to_cart_conv,
          cart_to_order_conv: d.cart_to_order_conv,
          ad_ctr: d.ad_ctr,
          ad_cpc: d.ad_cpc,
        });
      }
    }
    setResults(res);
  };

  return (
    <div className="p-4 font-sans text-gray-800">
      <div className="flex justify-between items-center mb-4">
        <h1 className="text-xl font-semibold">Сравнение карточек</h1>
        <button
          onClick={goBack}
          className="px-3 py-1 rounded bg-gray-200 hover:bg-gray-300"
        >
          Назад
        </button>
      </div>

      <div className="mb-4 flex flex-wrap items-end gap-2">
        <input
          type="date"
          value={start}
          onChange={(e) => setStart(e.target.value)}
          className="border p-1 rounded"
        />
        <input
          type="date"
          value={end}
          onChange={(e) => setEnd(e.target.value)}
          className="border p-1 rounded"
        />
        {groups.map((g, idx) => (
          <div key={idx} className="flex items-center gap-2">
            <select
              value={g.type}
              onChange={(e) => handleChange(idx, "type", e.target.value)}
              className="border p-1 rounded"
            >
              {filterTypes.map((ft) => (
                <option key={ft.value} value={ft.value}>
                  {ft.label}
                </option>
              ))}
            </select>
            {g.type === "ad_manager_name" || g.type === "card_changes" || g.type === "brand" ? (
              <select
                value={g.value}
                onChange={(e) => handleChange(idx, "value", e.target.value)}
                className="border p-1 rounded"
              >
                <option value="">Выберите</option>
                {g.type === "ad_manager_name" &&
                  managerOpts.map((m) => (
                    <option key={m} value={m}>
                      {m === "0" ? "Без менеджера" : m}
                    </option>
                  ))}
                {g.type === "card_changes" &&
                  changeOpts.map((o) => (
                    <option key={o} value={o}>
                      {o}
                    </option>
                  ))}
                {g.type === "brand" &&
                  brandOpts.map((b) => (
                    <option key={b} value={b}>
                      {b}
                    </option>
                  ))}
              </select>
            ) : (
              <input
                type="text"
                value={g.value}
                onChange={(e) => handleChange(idx, "value", e.target.value)}
                className="border p-1 rounded"
              />
            )}
          </div>
        ))}
        <button
          type="button"
          onClick={addGroup}
          className="flex items-center px-3 py-2 bg-gray-100 hover:bg-gray-200 rounded border border-gray-300 text-sm"
        >
          <span className="mr-1">+</span> Добавить группу
        </button>
        <button
          onClick={fetchData}
          className="px-4 py-2 bg-blue-600 hover:bg-blue-700 text-white font-semibold rounded"
        >
          Сравнить
        </button>
      </div>

      <hr className="my-4 border-gray-300" />

      {results.length > 0 && (
        <div className="mt-6 overflow-x-auto">
          <table className="min-w-full divide-y divide-gray-200 border text-sm">
            <thead className="bg-gray-50">
              <tr>
                <th className="px-4 py-2 text-left">Группа</th>
                <th className="px-4 py-2 text-left">Чистая прибыль</th>
                <th className="px-4 py-2 text-left">Маржа %</th>
                <th className="px-4 py-2 text-left">Реклама</th>
                <th className="px-4 py-2 text-left">ATC %</th>
                <th className="px-4 py-2 text-left">Cart→Order %</th>
                <th className="px-4 py-2 text-left">Ad CTR %</th>
                <th className="px-4 py-2 text-left">Ad CPC</th>
              </tr>
            </thead>
            <tbody className="bg-white divide-y divide-gray-200">
              {results.map((r) => (
                <tr key={r.label} className="hover:bg-gray-50">
                  <td className="px-4 py-2 font-medium whitespace-nowrap">{r.label}</td>
                  <td className="px-4 py-2 whitespace-nowrap">{r.total_profit.toFixed(2)} ₽</td>
                  <td className="px-4 py-2 whitespace-nowrap">{r.margin_percent.toFixed(2)} %</td>
                  <td className="px-4 py-2 whitespace-nowrap">{r.total_ad_spend.toFixed(2)} ₽</td>
                  <td className="px-4 py-2 whitespace-nowrap">{r.add_to_cart_conv.toFixed(2)}%</td>
                  <td className="px-4 py-2 whitespace-nowrap">{r.cart_to_order_conv.toFixed(2)}%</td>
                  <td className="px-4 py-2 whitespace-nowrap">{r.ad_ctr.toFixed(2)}%</td>
                  <td className="px-4 py-2 whitespace-nowrap">{r.ad_cpc.toFixed(2)} ₽</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
};

export default CardComparison;
