if [ $# -eq 0 ]
then
	echo "\n Available commands:"
	echo "   list : list all available runs"
	echo "   run + {int} : restart the provided run\n"

elif [[ $1 == "run" && $2 -eq 0 ]]
then
	echo " [Error] - Provide run index"

elif [[ $1 == "run" && $2 -gt 0 ]]
then
	echo "Re-starting run" $2
	# Change permissions on site-packages directory
	path="$(python -c "from ram.utils.packages import find_installed_ram; print find_installed_ram()")"
	path=$path"/ram/strategy/"
	sudo chmod -R a+rwx $path
	# Move original implementation of strategy to temp folder, and delete
	cp -R $GITHUB/ram/ram/strategy/long_pead/ $GITHUB/ram/ram/strategy/long_pead_TEMP/
	sleep 1
	rm -r $GITHUB/ram/ram/strategy/long_pead/
	sleep 1
	# Copy run source code from simulations directory
	python $GITHUB/ram/ram/strategy/long_pead_TEMP/implementation/manage_strategy_source_code.py -c -cr $2
	sleep 1
	# Remove long_pead from site-packages directory
	python $GITHUB/ram/ram/strategy/long_pead_TEMP/implementation/manage_strategy_source_code.py -c --delete_strategy_source_code
	sleep 1
	# Build from implementation setup script.
	sudo python $GITHUB/ram/setup.py clean --all
	sleep 1
	sudo python $GITHUB/ram/ram/strategy/long_pead_TEMP/implementation/source_code_setup.py install
	sleep 1
	# Restart run
	python $GITHUB/ram/ram/strategy/long_pead/main.py -r $2
	sleep 1
	# Remove run source code and replace with original, and rebuild
	rm -r $GITHUB/ram/ram/strategy/long_pead/
	sleep 1
	cp -R $GITHUB/ram/ram/strategy/long_pead_TEMP/ $GITHUB/ram/ram/strategy/long_pead/
	sleep 1
	rm -r $GITHUB/ram/ram/strategy/long_pead_TEMP/
	sleep 1
	sudo python $GITHUB/ram/setup.py clean --all
	sleep 1
	sudo python $GITHUB/ram/setup.py install

elif [ $1 == "list" ]
then
	python $GITHUB/ram/ram/strategy/long_pead/implementation/manage_strategy_source_code.py -c -lr

else
	echo "Incorrect arguments"
fi
