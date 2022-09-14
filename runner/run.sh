source /root/gaolei/venv_gl.sh
echo "任务执行"
echo "w2v running!"
cd /root/reco/w2v/ && python3 w2v_click.py >w2v.log &
echo "swing running!"
cd /root/reco/swing/ && python3 swing_click.py >swing.log
echo "emnsemble running!"
cd /root/reco/runner && python3 ensemble_v2.py >ensemble_v2.log 
#cd /root/reco/runner && python3 ensemble.py >ensemble.log 
echo "res_to_hive running!"
cd /root/reco/runner && python3 res_to_hive.py >res_to_hive.log 
echo "post data running!"
cd /root/reco/runner && python3 post_res.py --type=post >post_data.log 
echo "get data running!"
cd /root/reco/runner && python3 post_res.py --type=get >get_data.log 
echo "cp res bak"
cd /root/gaolei/InferfaceProject && sh res_bak.sh >res_bak.log
#cd /root/reco/runner && python3 test.py > test.log
echo "done!"

if [ $? -ne 0 ];then
	mail -s "w2v 任务失败"  1787713921@qq.com 

	echo "wrong"
	echo "---------------------------------------------------------------------------------------------"
	echo $output
fi
