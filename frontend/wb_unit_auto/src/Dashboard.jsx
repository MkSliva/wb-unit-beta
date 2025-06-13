import React, { useState, useEffect, useCallback, useMemo } from "react";
import {
  LineChart,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  ResponsiveContainer,
} from "recharts";

const Dashboard = ({ openEconomics, openMissing }) => {
  const [startDate, setStartDate] = useState("");
  const [endDate, setEndDate] = useState("");
  const [totalProfit, setTotalProfit] = useState(0);
  const [groupedSales, setGroupedSales] = useState([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);
  const [totalAdSpend, setTotalAdSpend] = useState(0);
  const [totalOrders, setTotalOrders] = useState(0);
  const [missingCosts, setMissingCosts] = useState([]);

  const [isModalOpen, setIsModalOpen] = useState(false);
  const [selectedImt, setSelectedImt] = useState(null);
  const [groupDetails, setGroupDetails] = useState([]);
  const [dailySales, setDailySales] = useState([]);
  const [editData, setEditData] = useState({});

  // *** НОВЫЕ СОСТОЯНИЯ ДЛЯ ДАННЫХ ГРАФИКОВ ***
  const [purchasePriceHistory, setPurchasePriceHistory] = useState([]);

  const [overallDaily, setOverallDaily] = useState([]);
  const [latestCosts, setLatestCosts] = useState({});
  const [hoveredVendor, setHoveredVendor] = useState(null);
  const [purchaseBatches, setPurchaseBatches] = useState({});

  // *** СОСТОЯНИЕ ДЛЯ СОРТИРОВКИ ***
  const [sortConfig, setSortConfig] = useState({ key: null, direction: "ascending" });

  const [newBatchData, setNewBatchData] = useState({
    vendor_code: "",
    purchase_price: "",
    quantity_bought: "",
    start_date: new Date().toISOString().split("T")[0],
  });

  useEffect(() => {
    const today = new Date();
    const thirtyDaysAgo = new Date(today);
    thirtyDaysAgo.setDate(today.getDate() - 30);

    setEndDate(today.toISOString().split("T")[0]);
    setStartDate(thirtyDaysAgo.toISOString().split("T")[0]);
  }, []);

  const fetchData = useCallback(async () => {
    if (!startDate || !endDate) return;

    setLoading(true);
    setError(null);

    const queryParams = new URLSearchParams({
      start_date: startDate,
      end_date: endDate,
    });

    try {
      const response = await fetch(
        `http://localhost:8000/api/sales_grouped_detailed_range?${queryParams.toString()}`
      );
      if (!response.ok) {
        const errorText = await response.text();
        throw new Error(
          `HTTP error! status: ${response.status}, message: ${errorText}`
        );
      }
      const data = await response.json();
      setGroupedSales(data.data);
      setTotalProfit(data.total_profit);
      setTotalAdSpend(data.total_ad_spend);
      setTotalOrders(data.total_orders);

      const dailyResp = await fetch(
        `http://localhost:8000/api/sales_overall_daily?start_date=${startDate}&end_date=${endDate}`
      );
      if (dailyResp.ok) {
        const dailyData = await dailyResp.json();
        setOverallDaily(dailyData.data);
      } else {
        setOverallDaily([]);
      }

      const missingResp = await fetch(
        `http://localhost:8000/api/missing_costs?start_date=${startDate}&end_date=${endDate}`
      );
      if (missingResp.ok) {
        const m = await missingResp.json();
        setMissingCosts(m);
      } else {
        setMissingCosts([]);
      }
    } catch (err) {
      console.error("Error fetching grouped sales:", err);
      setError("Не удалось загрузить данные о продажах. " + err.message);
    } finally {
      setLoading(false);
    }
  }, [startDate, endDate]);

  useEffect(() => {
    fetchData();
  }, [fetchData]);

  // *** ФУНКЦИЯ ДЛЯ ОБРАБОТКИ СОРТИРОВКИ ***
  const requestSort = (key) => {
    let direction = "ascending";
    if (sortConfig.key === key && sortConfig.direction === "ascending") {
      direction = "descending";
    }
    setSortConfig({ key, direction });
  };

  // *** МЕМОИЗИРОВАННЫЕ ОТСОРТИРОВАННЫЕ ДАННЫЕ ***
  const sortedGroupedSales = useMemo(() => {
    let sortableItems = [...groupedSales];
    if (sortConfig.key !== null) {
      sortableItems.sort((a, b) => {
        const valA = a[sortConfig.key];
        const valB = b[sortConfig.key];

        if (valA === null || isNaN(valA) || typeof valA === "undefined")
          return sortConfig.direction === "ascending" ? 1 : -1;
        if (valB === null || isNaN(valB) || typeof valB === "undefined")
          return sortConfig.direction === "ascending" ? -1 : 1;

        if (valA < valB) {
          return sortConfig.direction === "ascending" ? -1 : 1;
        }
        if (valA > valB) {
          return sortConfig.direction === "ascending" ? 1 : -1;
        }
        return 0;
      });
    }
    return sortableItems;
  }, [groupedSales, sortConfig]);

  const fetchGroupDetails = useCallback(
    async (imtId) => {
      setLoading(true);
      setError(null);
      try {
        const detailsResponse = await fetch(
          `http://localhost:8000/api/sales_by_imt?imt_id=${imtId}&start_date=${startDate}&end_date=${endDate}`
        );
        if (!detailsResponse.ok) {
          throw new Error(`HTTP error! status: ${detailsResponse.status}`);
        }
        const detailsData = await detailsResponse.json();
        setGroupDetails(detailsData.data);

        const batchPromises = detailsData.data.map((item) =>
          fetchPurchaseBatches(item.vendorcode)
        );
        await Promise.all(batchPromises);

        // Получение данных для ежедневных графиков (прибыль, заказы, реклама)
        const dailyResponse = await fetch(
          `http://localhost:8000/api/sales_by_imt_daily?imt_id=${imtId}&start_date=${startDate}&end_date=${endDate}`
        );
        if (!dailyResponse.ok) {
          throw new Error(`HTTP error! status: ${dailyResponse.status}`);
        }
        const dailyData = await dailyResponse.json();
        setDailySales(dailyData.data);
        console.log("Daily Sales Data:", dailyData.data); // Отладка

        // Получение данных для графика истории закупочной цены
        let vendorCodeForHistory = '';
        if (detailsData.data.length > 0) {
          // Попробуем найти vendorcode, который не пустой
          const firstValidVendor = detailsData.data.find(item => item.vendorcode);
          if (firstValidVendor) {
            vendorCodeForHistory = firstValidVendor.vendorcode;
          }
        }
        console.log("Vendor Code for Purchase Price History:", vendorCodeForHistory); // Отладка

        if (vendorCodeForHistory) {
          const ppHistoryResponse = await fetch(
            `http://localhost:8000/api/purchase_price_history_daily?vendor_code=${vendorCodeForHistory}&start_date=${startDate}&end_date=${endDate}`
          );
          if (!ppHistoryResponse.ok) {
            // Обработка ошибки, если история цены не получена
            const errorText = await ppHistoryResponse.text();
            console.error("Error fetching purchase price history:", errorText);
            throw new Error(`HTTP error! status: ${ppHistoryResponse.status}, message: ${errorText}`);
          }
          const ppHistoryData = await ppHistoryResponse.json();
          setPurchasePriceHistory(ppHistoryData);
          console.log("Purchase Price History Data:", ppHistoryData); // Отладка
        } else {
          setPurchasePriceHistory([]); // Очищаем, если нет vendorCode
          console.log("No valid vendorcode found for purchase price history."); // Отладка
        }


        if (detailsData.data.length > 0) {
          const firstVendorData = detailsData.data[0];
          setEditData({
            vendorcode: firstVendorData.vendorcode || "",
            purchase_price: firstVendorData.purchase_price || 0,
            delivery_to_warehouse: firstVendorData.delivery_to_warehouse || 0,
            wb_commission_rub: firstVendorData.wb_commission_rub || 0,
            wb_logistics: firstVendorData.wb_logistics || 0,
            tax_rub: firstVendorData.tax_rub || 0,
            packaging: firstVendorData.packaging || 0,
            fuel: firstVendorData.fuel || 0,
            gift: firstVendorData.gift || 0,
            real_defect_percent: firstVendorData.real_defect_percent || 0,
            start_date: startDate,
          });
          fetchLatestCost(firstVendorData.vendorcode);
        }
      } catch (err) {
        console.error("Error fetching group details or price history:", err); // Обновлено сообщение об ошибке
        setError("Не удалось загрузить детали группы или историю цен. " + err.message);
      } finally {
        setLoading(false);
      }
    },
    [startDate, endDate]
  );

  const openModal = (imtId) => {
    setSelectedImt(imtId);
    fetchGroupDetails(imtId);
    setIsModalOpen(true);
  };

  const closeModal = () => {
    setIsModalOpen(false);
    setSelectedImt(null);
    setGroupDetails([]);
    setDailySales([]);
    setPurchasePriceHistory([]);
    setPurchaseBatches({});
    setEditData({});
    setError(null);
  };

  const handleEditChange = (e) => {
    const { name, value } = e.target;
    setEditData((prev) => ({
      ...prev,
      [name]: ["vendorcode", "start_date"].includes(name)
        ? value
        : parseFloat(value) || 0,
    }));
  };

  const handleNewBatchChange = (e) => {
    const { name, value } = e.target;
    setNewBatchData((prev) => ({ ...prev, [name]: value }));
  };

  const submitCostUpdate = async () => {
    const dataToSend = { ...editData, start_date: editData.start_date || startDate };

    try {
      const response = await fetch("http://localhost:8000/api/update_costs", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(dataToSend),
      });
      if (response.ok) {
        alert("Данные себестоимости обновлены!");
        fetchData();
        if (selectedImt) {
          fetchGroupDetails(selectedImt);
        }
      } else {
        const errorData = await response.json();
        console.error("Ошибка при обновлении себестоимости:", errorData);
        alert(
          `Ошибка при обновлении себестоимости: ${
            JSON.stringify(errorData.detail || errorData.message)
          }`
        );
      }
    } catch (error) {
      console.error("Сетевая ошибка при обновлении себестоимости:", error);
      alert("Произошла сетевая ошибка. Проверьте консоль.");
    }
  };

  const submitNewBatch = async () => {
    if (
      !newBatchData.vendor_code ||
      !newBatchData.purchase_price ||
      !newBatchData.quantity_bought ||
      !newBatchData.start_date
    ) {
      alert("Пожалуйста, заполните все поля для новой партии.");
      return;
    }

    const dataToSend = {
      ...newBatchData,
      purchase_price: parseFloat(newBatchData.purchase_price),
      quantity_bought: parseInt(newBatchData.quantity_bought, 10),
    };

    try {
      const response = await fetch("http://localhost:8000/api/purchase_batches", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(dataToSend),
      });
      if (response.ok) {
        console.log("Новая партия успешно добавлена!");
        alert("Новая закупочная партия добавлена!");
        setNewBatchData({
          vendor_code: "",
          purchase_price: "",
          quantity_bought: "",
          start_date: new Date().toISOString().split("T")[0],
        });
        fetchData();
        if (selectedImt) {
          fetchGroupDetails(selectedImt);
        }
      } else {
        const errorData = await response.json();
        console.error("Ошибка при добавлении новой партии:", errorData);
        alert(
          `Ошибка при добавлении партии: ${
            JSON.stringify(errorData.detail || errorData.message)
          }`
        );
      }
    } catch (error) {
      console.error("Сетевая ошибка при добавлении новой партии:", error);
      alert("Произошла сетевая ошибка. Проверьте консоль.");
    }
  };

  const checkBatches = async () => {
    try {
      const response = await fetch("http://localhost:8000/api/check_purchase_batches");
      if (response.ok) {
        const result = await response.json();
        alert(
          `Проверка партий завершена. Деактивировано: ${
            result.deactivated_batches.join(", ") || "ничего"
          }. Проверьте Telegram.`
        );
        console.log("Check batches result:", result);
        fetchData();
        if (selectedImt) {
          fetchGroupDetails(selectedImt);
        }
      } else {
        const errorData = await response.json();
        console.error("Ошибка при проверке партий:", errorData);
        alert(
          `Ошибка при проверке партий: ${
            JSON.stringify(errorData.detail || errorData.message)
          }`
        );
      }
    } catch (error) {
      console.error("Сетевая ошибка при проверке партий:", error);
      alert("Произошла сетевая ошибка. Проверьте консоль.");
    }
  };

  const fetchLatestCost = async (vendorCode) => {
    try {
      const response = await fetch(
        `http://localhost:8000/api/latest_costs?vendor_code=${vendorCode}`
      );
      if (response.ok) {
        const data = await response.json();
        setLatestCosts((prev) => ({ ...prev, [vendorCode]: data }));
        setEditData((prev) => ({
          ...prev,
          vendorcode: vendorCode,
          purchase_price: data.purchase_price || 0,
          delivery_to_warehouse: data.delivery_to_warehouse || 0,
          wb_commission_rub: data.wb_commission_rub || 0,
          wb_logistics: data.wb_logistics || 0,
          tax_rub: data.tax_rub || 0,
          packaging: data.packaging || 0,
          fuel: data.fuel || 0,
          gift: data.gift || 0,
          real_defect_percent: data.real_defect_percent || 0,
        }));
      }
    } catch (error) {
      console.error("Error fetching latest costs:", error);
    }
  };

  const handleVendorSelect = async (e) => {
    const vendor = e.target.value;
    await fetchLatestCost(vendor);
  };

  const fetchPurchaseBatches = async (vendorCode) => {
    try {
      const response = await fetch(
        `http://localhost:8000/api/purchase_batches?vendor_code=${vendorCode}`
      );
      if (response.ok) {
        const data = await response.json();
        setPurchaseBatches((prev) => ({ ...prev, [vendorCode]: data }));
      }
    } catch (error) {
      console.error("Error fetching purchase batches:", error);
    }
  };

  // Вспомогательная функция для получения индикатора сортировки
  const getSortIndicator = (key) => {
    if (sortConfig.key === key) {
      return sortConfig.direction === "ascending" ? " ▲" : " ▼";
    }
    return "";
  };

  return (
    <div className="container mx-auto p-4">
      <div className="flex justify-between items-center mb-4">
        <h1 className="text-2xl font-bold">WB Аналитика Продаж</h1>
        <button
          onClick={openEconomics}
          className="px-4 py-2 bg-indigo-600 text-white rounded hover:bg-indigo-700"
        >
          Данные Юнит Экономики
        </button>
      </div>

      <div className="flex space-x-4 mb-4">
        <div>
          <label
            htmlFor="startDate"
            className="block text-sm font-medium text-gray-700"
          >
            Начальная дата:
          </label>
          <input
            type="date"
            id="startDate"
            value={startDate}
            onChange={(e) => setStartDate(e.target.value)}
            className="mt-1 block w-full rounded-md border-gray-300 shadow-sm focus:border-indigo-300 focus:ring focus:ring-indigo-200 focus:ring-opacity-50"
          />
        </div>
        <div>
          <label
            htmlFor="endDate"
            className="block text-sm font-medium text-gray-700"
          >
            Конечная дата:
          </label>
          <input
            type="date"
            id="endDate"
            value={endDate}
            onChange={(e) => setEndDate(e.target.value)}
            className="mt-1 block w-full rounded-md border-gray-300 shadow-sm focus:border-indigo-300 focus:ring focus:ring-indigo-200 focus:ring-opacity-50"
          />
        </div>
        <button
          onClick={fetchData}
          className="mt-6 px-4 py-2 bg-blue-500 text-white rounded-md hover:bg-blue-600 focus:outline-none focus:ring-2 focus:ring-blue-500 focus:ring-opacity-50"
          disabled={loading}
        >
          {loading ? "Загрузка..." : "Применить фильтр"}
        </button>
      </div>

      {error && <div className="text-red-600 mb-4">{error}</div>}
      <div className="mb-4 grid grid-cols-1 sm:grid-cols-3 gap-4">
        <div className="p-3 bg-gray-50 rounded shadow">
          <div className="text-sm text-gray-500">Общая прибыль</div>
          <div className={`text-xl font-semibold ${totalProfit >= 0 ? "text-green-600" : "text-red-600"}`}>{totalProfit.toFixed(2)} руб.</div>
        </div>
        <div className="p-3 bg-gray-50 rounded shadow">
          <div className="text-sm text-gray-500">Реклама</div>
          <div className="text-xl font-semibold">{totalAdSpend.toFixed(2)} руб.</div>
        </div>
        <div className="p-3 bg-gray-50 rounded shadow">
          <div className="text-sm text-gray-500">Заказы</div>
          <div className="text-xl font-semibold">{totalOrders}</div>
        </div>
      </div>

      <div className="mb-6">
          <div style={{ width: "100%", height: 300 }}>
            <ResponsiveContainer>
              <LineChart
                data={overallDaily}
                margin={{ top: 5, right: 30, left: 20, bottom: 5 }}
              >
                <CartesianGrid strokeDasharray="3 3" />
                <XAxis dataKey="date" />
                <YAxis yAxisId="left" orientation="left" />
                <YAxis yAxisId="right" orientation="right" />
                <Tooltip />
                <Legend />
                <Line
                  yAxisId="left"
                  type="monotone"
                  dataKey="orderscount"
                  stroke="#82ca9d"
                  name="Заказы"
                />
                <Line
                  yAxisId="left"
                  type="monotone"
                  dataKey="total_profit"
                  stroke="#8884d8"
                  name="Прибыль"
                />
                <Line
                  yAxisId="right"
                  type="monotone"
                  dataKey="ad_spend"
                  stroke="#ffc658"
                  name="Реклама"
                />
              </LineChart>
            </ResponsiveContainer>
          </div>
        ) : (
          <p className="text-gray-600">Нет данных для графика.</p>
        )}
      </div>

      {missingCosts.length > 0 && (
        <div className="bg-red-200 text-red-800 p-4 mb-4">
          <p>
            Внимание! У некоторых заказанных товаров не заполнена закупочная
            цена! Данные сайта могут быть неточными!
          </p>
          <button
            onClick={() =>
              openMissing({ start: startDate, end: endDate })
            }
            className="underline mt-2"
          >
            Посмотреть
          </button>
        </div>
      )}

      <div className="overflow-x-auto">
        <table className="min-w-full divide-y divide-gray-200 shadow-md rounded-lg">
          <thead className="bg-gray-50">
            <tr>
              <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                IMT ID
              </th>
              <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider max-w-[200px]">
                Артикулы
              </th>
              <th
                className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider cursor-pointer hover:bg-gray-100"
                onClick={() => requestSort("orderscount")}
              >
                Заказы {getSortIndicator("orderscount")}
              </th>
              <th
                className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider cursor-pointer hover:bg-gray-100"
                onClick={() => requestSort("ad_spend")}
              >
                Реклама {getSortIndicator("ad_spend")}
              </th>
              <th
                className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider cursor-pointer hover:bg-gray-100"
                onClick={() => requestSort("total_profit")}
              >
                Прибыль {getSortIndicator("total_profit")}
              </th>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider cursor-pointer hover:bg-gray-100"
                onClick={() => requestSort("revenue_percent")}>
                  Доля выручки % {getSortIndicator("revenue_percent")}
                </th>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider cursor-pointer hover:bg-gray-100"
                onClick={() => requestSort("profit_percent")}>
                  Доля прибыли % {getSortIndicator("profit_percent")}
                </th>
              <th
                className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider cursor-pointer hover:bg-gray-100"
                onClick={() => requestSort("margin_percent")}
              >
                Маржа % {getSortIndicator("margin_percent")}
              </th>
              <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                Действия
              </th>
            </tr>
          </thead>
          <tbody className="bg-white divide-y divide-gray-200">
            {sortedGroupedSales.map((group) => (
              <tr key={group.imtid}>
                <td className="px-6 py-4 whitespace-nowrap text-sm font-medium text-gray-900">
                  {group.imtid}
                </td>
                <td
                  className="px-6 py-4 whitespace-nowrap text-sm text-gray-500 max-w-[200px] truncate"
                  title={group.vendorcodes}
                >
                  {group.vendorcodes}
                </td>
                <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                  {group.orderscount}
                </td>
                <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                  {group.ad_spend.toFixed(2)}
                </td>
                <td
                  className={`px-6 py-4 whitespace-nowrap text-sm ${
                    group.total_profit >= 0 ? "text-green-600" : "text-red-600"
                  }`}
                >
                  {group.total_profit.toFixed(2)}
                </td>
                <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                  {group.revenue_percent ? group.revenue_percent.toFixed(2) + " %" : "-"}
                </td>
                <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                  {group.profit_percent ? group.profit_percent.toFixed(2) + " %" : "-"}
                </td>
                <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                  {group.margin_percent ? group.margin_percent.toFixed(2) + " %" : "-"}
                </td>
                <td className="px-6 py-4 whitespace-nowrap text-right text-sm font-medium">
                  <button
                    onClick={() => openModal(group.imtid)}
                    className="text-indigo-600 hover:text-indigo-900"
                  >
                    Подробнее
                  </button>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      {isModalOpen && (
        <div className="fixed inset-0 bg-gray-600 bg-opacity-50 overflow-y-auto h-full w-full flex justify-center items-center z-50">
          <div className="bg-white p-6 rounded-lg shadow-xl w-11/12 max-w-5xl max-h-[95vh] overflow-y-auto">
            <h3 className="text-xl font-bold mb-4">
              Детали по IMT ID: {selectedImt}
            </h3>

            {loading ? (
              <div>Загрузка деталей...</div>
            ) : error ? (
              <div className="text-red-600">{error}</div>
            ) : (
              <>
                <div className="mt-6 p-4 border rounded-lg bg-gray-50 mb-4">
                  <h4 className="font-semibold mb-2">
                    Добавить новую закупочную партию
                  </h4>
                  <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
                    <label className="block">
                      Артикул (vendorCode):
                      <input
                        type="text"
                        name="vendor_code"
                        className="border p-2 rounded-md w-full mt-1"
                        value={newBatchData.vendor_code}
                        onChange={handleNewBatchChange}
                        list="vendorcodes-list"
                      />
                      <datalist id="vendorcodes-list">
                        {groupDetails.map((item) => (
                          <option key={item.vendorcode} value={item.vendorcode} />
                        ))}
                      </datalist>
                    </label>
                    <label className="block">
                      Закупочная цена (руб):
                      <input
                        type="number"
                        name="purchase_price"
                        className="border p-2 rounded-md w-full mt-1"
                        value={newBatchData.purchase_price}
                        onChange={handleNewBatchChange}
                        min="0"
                        step="0.01"
                      />
                    </label>
                    <label className="block">
                      Количество шт. в партии:
                      <input
                        type="number"
                        name="quantity_bought"
                        className="border p-2 rounded-md w-full mt-1"
                        value={newBatchData.quantity_bought}
                        onChange={handleNewBatchChange}
                        min="1"
                        step="1"
                      />
                    </label>
                    <label className="block">
                      Дата начала действия:
                      <input
                        type="date"
                        name="start_date"
                        className="border p-2 rounded-md w-full mt-1"
                        value={newBatchData.start_date}
                        onChange={handleNewBatchChange}
                      />
                    </label>
                  </div>
                  <button
                    onClick={submitNewBatch}
                    className="mt-4 bg-indigo-600 text-white px-4 py-2 rounded-md hover:bg-indigo-700 focus:outline-none focus:ring-2 focus:ring-indigo-500 focus:ring-opacity-50"
                  >
                    Добавить партию
                  </button>
                </div>

                <button
                  onClick={checkBatches}
                  className="mt-4 mb-4 bg-orange-500 text-white px-4 py-2 rounded-md hover:bg-orange-600 focus:outline-none focus:ring-2 focus:ring-orange-500 focus:ring-opacity-50"
                >
                  Проверить партии сейчас (отправить уведомления)
                </button>

                <h4 className="text-lg font-semibold mb-2">
                  Детали по артикулам для IMT ID {selectedImt}:
                </h4>
                <div className="overflow-x-auto mb-6">
                  <table className="min-w-full divide-y divide-gray-200 border">
                    <thead className="bg-gray-50">
                      <tr>
                        <th className="px-4 py-2 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                          Артикул
                        </th>
                        <th className="px-4 py-2 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                          Заказы
                        </th>
                        <th className="px-4 py-2 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                          Реклама
                        </th>
                        <th className="px-4 py-2 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                          Прибыль
                        </th>
                        <th className="px-4 py-2 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                          Ср. Цена продажи
                        </th>
                        <th className="px-4 py-2 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                          Ср. Себестоимость
                        </th>
                        <th className="px-4 py-2 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                          Ср. Закупка
                        </th>
                      </tr>
                    </thead>
                    <tbody className="bg-white divide-y divide-gray-200">
                      {groupDetails.map((detail) => (
                        <tr key={detail.vendorcode}>
                          <td
                            className="px-4 py-2 whitespace-nowrap text-sm font-medium text-gray-900 relative"
                            onMouseEnter={() => {
                              setHoveredVendor(detail.vendorcode);
                              if (!latestCosts[detail.vendorcode]) {
                                fetchLatestCost(detail.vendorcode);
                              }
                            }}
                            onMouseLeave={() => setHoveredVendor(null)}
                          >
                            {detail.vendorcode}
                            {hoveredVendor === detail.vendorcode && latestCosts[detail.vendorcode] && (
                              <div className="absolute left-0 top-full mt-1 p-2 bg-white border rounded shadow-lg text-xs z-10">
                                <div>Закупка: {latestCosts[detail.vendorcode].purchase_price}</div>
                                <div>Доставка: {latestCosts[detail.vendorcode].delivery_to_warehouse}</div>
                                <div>Комиссия: {latestCosts[detail.vendorcode].wb_commission_rub}</div>
                                <div>Логистика: {latestCosts[detail.vendorcode].wb_logistics}</div>
                                <div>Налог: {latestCosts[detail.vendorcode].tax_rub}</div>
                                <div>Упаковка: {latestCosts[detail.vendorcode].packaging}</div>
                                <div>Топливо: {latestCosts[detail.vendorcode].fuel}</div>
                                <div>Подарок: {latestCosts[detail.vendorcode].gift}</div>
                              </div>
                            )}
                          </td>
                          <td className="px-4 py-2 whitespace-nowrap text-sm text-gray-500">
                            {detail.orderscount}
                          </td>
                          <td className="px-4 py-2 whitespace-nowrap text-sm text-gray-500">
                            {detail.ad_spend.toFixed(2)}
                          </td>
                          <td
                            className={`px-4 py-2 whitespace-nowrap text-sm ${
                              detail.total_profit >= 0 ? "text-green-600" : "text-red-600"
                            }`}
                          >
                            {detail.total_profit.toFixed(2)}
                          </td>
                          <td className="px-4 py-2 whitespace-nowrap text-sm text-gray-500">
                            {typeof detail.actual_discounted_price === "number" && !isNaN(detail.actual_discounted_price)
                              ? detail.actual_discounted_price.toFixed(2) + " ₽"
                              : "0.00 ₽"}
                          </td>
                          <td className="px-4 py-2 whitespace-nowrap text-sm text-gray-500">
                            {typeof detail.cost_price === "number" && !isNaN(detail.cost_price)
                              ? detail.cost_price.toFixed(2) + " ₽"
                              : "0.00 ₽"}
                          </td>
                          <td className="px-4 py-2 whitespace-nowrap text-sm text-gray-500">
                            {typeof detail.purchase_price === "number" && !isNaN(detail.purchase_price)
                              ? detail.purchase_price.toFixed(2) + " ₽"
                              : "0.00 ₽"}
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>

                <div className="mt-4 p-4 border rounded-lg bg-gray-50">
                  <h4 className="font-semibold mb-2">
                    Изменить себестоимость (другие расходы)
                  </h4>
                  <div className="grid grid-cols-1 md:grid-cols-3 lg:grid-cols-4 gap-3">
                    <label className="block">
                      Артикул для обновления:
                      <select
                        name="vendorcode"
                        className="border p-2 rounded-md w-full mt-1"
                        value={editData.vendorcode || ""}
                        onChange={handleVendorSelect}
                      >
                        {groupDetails.map((item) => (
                          <option key={item.vendorcode} value={item.vendorcode}>
                            {item.vendorcode}
                          </option>
                        ))}
                      </select>
                    </label>
                    <label className="block">
                      Дата начала действия:
                      <input
                        type="date"
                        name="start_date"
                        className="border p-2 rounded-md w-full mt-1"
                        value={editData.start_date || ""}
                        onChange={handleEditChange}
                      />
                    </label>
                    <label className="block">
                      Закупочная цена (руб):
                      <input
                        type="number"
                        name="purchase_price"
                        className="border p-2 rounded-md w-full mt-1"
                        value={editData.purchase_price || ""}
                        onChange={handleEditChange}
                        min="0"
                        step="0.01"
                      />
                    </label>
                    <label className="block">
                      Доставка на склад (руб):
                      <input
                        type="number"
                        name="delivery_to_warehouse"
                        className="border p-2 rounded-md w-full mt-1"
                        value={editData.delivery_to_warehouse || ""}
                        onChange={handleEditChange}
                        min="0"
                        step="0.01"
                      />
                    </label>
                    <label className="block">
                      Логистика WB (руб):
                      <input
                        type="number"
                        name="wb_logistics"
                        className="border p-2 rounded-md w-full mt-1"
                        value={editData.wb_logistics || ""}
                        onChange={handleEditChange}
                        min="0"
                        step="0.01"
                      />
                    </label>
                    <label className="block">
                      Упаковка (руб):
                      <input
                        type="number"
                        name="packaging"
                        className="border p-2 rounded-md w-full mt-1"
                        value={editData.packaging || ""}
                        onChange={handleEditChange}
                        min="0"
                        step="0.01"
                      />
                    </label>
                    <label className="block">
                      Топливо (руб):
                      <input
                        type="number"
                        name="fuel"
                        className="border p-2 rounded-md w-full mt-1"
                        value={editData.fuel || ""}
                        onChange={handleEditChange}
                        min="0"
                        step="0.01"
                      />
                    </label>
                    <label className="block">
                      Подарок (руб):
                      <input
                        type="number"
                        name="gift"
                        className="border p-2 rounded-md w-full mt-1"
                        value={editData.gift || ""}
                        onChange={handleEditChange}
                        min="0"
                        step="0.01"
                      />
                    </label>
                    <label className="block">
                      Процент брака (%):
                      <input
                        type="number"
                        name="real_defect_percent"
                        className="border p-2 rounded-md w-full mt-1"
                        value={editData.real_defect_percent || ""}
                        onChange={handleEditChange}
                        min="0"
                        step="0.01"
                      />
                    </label>
                  </div>
                <button
                    onClick={submitCostUpdate}
                    className="mt-4 bg-green-500 text-white px-4 py-2 rounded-md hover:bg-green-600 focus:outline-none focus:ring-2 focus:ring-green-500 focus:ring-opacity-50"
                  >
                    Обновить расходы для артикула
                  </button>
                </div>

                {/* --- Закупочные партии --- */}
                <div className="mt-6">
                  <h4 className="text-lg font-semibold mb-2">История закупочных партий</h4>
                  {Object.keys(purchaseBatches).map((vc) => (
                    <div key={vc} className="mb-4">
                      <h5 className="font-medium mb-1">Артикул {vc}</h5>
                      <table className="min-w-full divide-y divide-gray-200 border">
                        <thead className="bg-gray-50">
                          <tr>
                            <th className="px-2 py-1 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Начало</th>
                            <th className="px-2 py-1 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Конец</th>
                            <th className="px-2 py-1 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Цена</th>
                            <th className="px-2 py-1 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Кол-во</th>
                            <th className="px-2 py-1 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Продано</th>
                            <th className="px-2 py-1 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Статус</th>
                          </tr>
                        </thead>
                        <tbody className="bg-white divide-y divide-gray-200">
                          {purchaseBatches[vc]?.map((b) => (
                            <tr key={b.start_date} className={b.is_active ? 'bg-green-50' : ''}>
                              <td className="px-2 py-1 text-sm">{b.start_date}</td>
                              <td className="px-2 py-1 text-sm">{b.end_date || '-'}</td>
                              <td className="px-2 py-1 text-sm">{b.purchase_price}</td>
                              <td className="px-2 py-1 text-sm">{b.quantity_bought}</td>
                              <td className="px-2 py-1 text-sm">{b.quantity_sold}</td>
                              <td className="px-2 py-1 text-sm">{b.is_active ? 'активна' : 'закрыта'}</td>
                            </tr>
                          ))}
                        </tbody>
                      </table>
                    </div>
                  ))}
                </div>

                {/* --- ГРАФИКИ --- */}

                {/* График изменения закупочной цены */}
                <h4 className="text-lg font-semibold mb-2 mt-6">
                  История закупочной цены ({groupDetails[0]?.vendorcode || 'N/A'})
                </h4>
                {purchasePriceHistory.length > 0 ? (
                  <div style={{ width: "100%", height: 300 }}>
                    <ResponsiveContainer>
                      <LineChart
                        data={purchasePriceHistory}
                        margin={{ top: 5, right: 30, left: 20, bottom: 5 }}
                      >
                        <CartesianGrid strokeDasharray="3 3" />
                        <XAxis dataKey="date" />
                        <YAxis />
                        <Tooltip formatter={(value) => `${value.toFixed(2)} ₽`} />
                        <Legend />
                        <Line
                          type="monotone"
                          dataKey="purchase_price"
                          stroke="#8884d8"
                          activeDot={{ r: 8 }}
                          name="Закупочная цена"
                        />
                      </LineChart>
                    </ResponsiveContainer>
                  </div>
                ) : (
                  <p className="text-gray-600">
                    Данные по истории закупочной цены недоступны для выбранного артикула и периода.
                  </p>
                )}


                {/* График ежедневной динамики (Заказы, Прибыль, Реклама) */}
                <h4 className="text-lg font-semibold mb-2 mt-6">
                  Ежедневная динамика (Заказы, Прибыль, Реклама)
                </h4>
                {dailySales.length > 0 ? (
                  <div style={{ width: "100%", height: 300 }}>
                    <ResponsiveContainer>
                      <LineChart
                        data={dailySales}
                        margin={{ top: 5, right: 30, left: 20, bottom: 5 }}
                      >
                        <CartesianGrid strokeDasharray="3 3" />
                        <XAxis dataKey="date" />
                        <YAxis yAxisId="left" orientation="left" />
                        <YAxis yAxisId="right" orientation="right" />
                        <Tooltip />
                        <Legend />
                        <Line
                          yAxisId="left"
                          type="monotone"
                          dataKey="orderscount"
                          stroke="#82ca9d"
                          name="Заказы"
                        />
                        <Line
                          yAxisId="left"
                          type="monotone"
                          dataKey="total_profit"
                          stroke="#8884d8"
                          name="Прибыль"
                        />
                        <Line
                          yAxisId="right"
                          type="monotone"
                          dataKey="ad_spend"
                          stroke="#ffc658"
                          name="Реклама"
                        />
                      </LineChart>
                    </ResponsiveContainer>
                  </div>
                ) : (
                  <p className="text-gray-600">
                    Данные по ежедневной динамике недоступны для выбранного периода.
                  </p>
                )}


                <h4 className="text-lg font-semibold mb-2 mt-6">
                  Ежедневные продажи для IMT ID {selectedImt}:
                </h4>
                <div className="overflow-x-auto">
                  <table className="min-w-full divide-y divide-gray-200 border">
                    <thead className="bg-gray-50">
                      <tr>
                        <th className="px-4 py-2 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                          Дата
                        </th>
                        <th className="px-4 py-2 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                          Заказы
                        </th>
                        <th className="px-4 py-2 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                          Реклама
                        </th>
                        <th className="px-4 py-2 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                          Прибыль
                        </th>
                      </tr>
                    </thead>
                    <tbody className="bg-white divide-y divide-gray-200">
                      {dailySales.map((day) => (
                        <tr key={day.date}>
                          <td className="px-4 py-2 whitespace-nowrap text-sm font-medium text-gray-900">
                            {day.date}
                          </td>
                          <td className="px-4 py-2 whitespace-nowrap text-sm text-gray-500">
                            {day.orderscount}
                          </td>
                          <td className="px-4 py-2 whitespace-nowrap text-sm text-gray-500">
                            {day.ad_spend.toFixed(2)}
                          </td>
                          <td
                            className={`px-4 py-2 whitespace-nowrap text-sm ${
                              day.total_profit >= 0 ? "text-green-600" : "text-red-600"
                            }`}
                          >
                            {day.total_profit.toFixed(2)}
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </>
            )}

            <div className="mt-4 flex justify-end">
              <button
                onClick={closeModal}
                className="px-4 py-2 bg-red-500 text-white rounded-md hover:bg-red-600 focus:outline-none focus:ring-2 focus:ring-red-500 focus:ring-opacity-50"
              >
                Закрыть
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
};

export default Dashboard;