remove_original_src()
{
	# Move original implementation of strategy to temp folder, and delete
	cp -R $GITHUB/ram/ram/strategy/long_pead/ $GITHUB/ram/ram/strategy/long_pead_TEMP/
	sleep 1

	rm -r $GITHUB/ram/ram/strategy/long_pead/
	sleep 1

	# Clean old build directory
	sudo python $GITHUB/ram/setup.py clean --all
	sleep 1
}


replace_original_src()
{
	cp -R $GITHUB/ram/ram/strategy/long_pead_TEMP/ $GITHUB/ram/ram/strategy/long_pead/
	sleep 1

	rm -r $GITHUB/ram/ram/strategy/long_pead_TEMP/
	sleep 1

	sudo python $GITHUB/ram/setup.py clean --all
	sleep 1

	sudo python $GITHUB/ram/setup.py install &> /dev/null
}


clean_make_strategy()
{
	# Remove long_pead from site-packages directory
	python $GITHUB/ram/ram/strategy/long_pead_TEMP/implementation/training/manage_strategy_source_code.py --delete_strategy_source_code
	sleep 1

	echo "combo name " $1
	echo "run name" $2

	read -p "Press enter to continue: Copy original source code"

	# Copy run source code from simulations directory, and build to site-packages
	python $GITHUB/ram/ram/strategy/long_pead_TEMP/implementation/training/manage_strategy_source_code.py -cr $2
	sleep 1

	# Restart run
	python $GITHUB/ram/ram/strategy/long_pead/main.py -it $1 $2
	sleep 1

	# Remove run source code and replace with original, and rebuild
	rm -r $GITHUB/ram/ram/strategy/long_pead/
	sleep 1

}


# ~~~~~~ INTERFACE ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


if [ $# -eq 0 ]
then
	echo " Available commands:"
	echo "   combo + {str} : name of combo directory"

elif [[ $1 == "combo" && -z "$2" ]]
then
	echo " [Error] - Provide combo name"

elif [[ $1 == "combo" ]]
then
	combo_name=$2
	echo "combo name input " $combo_name

	# Change permissions on site-packages directory
	path="$(python -c "from ram.utils.packages import find_installed_ram; print find_installed_ram()")"
	path=$path"/strategy/"
	sudo chmod -R a+rwx $path

	# Move original source code in site-packages
	remove_original_src

	## HARD CODED RUNS
	#all_runs=(run_0106 run_0107 run_0108 run_0109 run_0110 run_0112 run_0113 run_0114 run_0115)
	all_runs=(run_0106)

	## LOOP THROUGH RUNS ##
	for run_name in ${all_runs[@]}
	do
		echo $run_name
		clean_make_strategy $combo_name $run_name
	done

	read -p "Press enter to continue: Replace original"
	# Move original back
	replace_original_src

elif [ $1 == "list" ]
then
	#python $GITHUB/ram/ram/strategy/long_pead/implementation/training/manage_strategy_source_code.py -c -lr
	echo "NOTHING HERE"
else
	echo "Incorrect arguments"
fi
