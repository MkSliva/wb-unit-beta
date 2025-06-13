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
    <div className="p-4">
      <div className="flex justify-between mb-4">
        <h1 className="text-xl font-bold">Сравнение карточек</h1>
        <button onClick={goBack} className="px-4 py-2 bg-gray-300 rounded">Назад</button>
      </div>
      <div className="mb-4 flex space-x-2">
        <input type="date" value={start} onChange={(e) => setStart(e.target.value)} className="border p-1" />
        <input type="date" value={end} onChange={(e) => setEnd(e.target.value)} className="border p-1" />
      </div>
      {groups.map((g, idx) => (
        <div key={idx} className="mb-2 flex space-x-2 items-center">
          <select
            value={g.type}
            onChange={(e) => handleChange(idx, "type", e.target.value)}
            className="border p-1"
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
              className="border p-1"
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
              className="border p-1"
            />
          )}
        </div>
      ))}
      <button onClick={addGroup} className="mt-2 px-2 py-1 bg-gray-200 rounded">+ Добавить группу</button>
      <div className="mt-4">
        <button onClick={fetchData} className="px-4 py-2 bg-blue-500 text-white rounded">Сравнить</button>
      </div>
      <div className="mt-4 grid grid-cols-1 md:grid-cols-2 gap-4">
        {results.map((r) => (
          <div key={r.label} className="border rounded p-4 bg-gray-50">
            <h3 className="font-semibold mb-2">{r.label}</h3>
            <div className="text-sm">Чистая прибыль: {r.total_profit.toFixed(2)} ₽</div>
            <div className="text-sm">Маржа от вложений: {r.margin_percent.toFixed(2)} %</div>
            <div className="text-sm">Рекламный расход: {r.total_ad_spend.toFixed(2)} ₽</div>
            <div className="text-sm">ATC конверсия: {r.add_to_cart_conv.toFixed(2)}%</div>
            <div className="text-sm">Cart→Order конверсия: {r.cart_to_order_conv.toFixed(2)}%</div>
            <div className="text-sm">Ad CTR: {r.ad_ctr.toFixed(2)}%</div>
            <div className="text-sm">Ad CPC: {r.ad_cpc.toFixed(2)} ₽</div>
          </div>
        ))}
      </div>
    </div>
  );
};

export default CardComparison;
